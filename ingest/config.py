"""Load a pipeline YAML: JSON Schema check, plugin checks, reference checks, topological order.

Validation is two-level. The JSON Schema checks the document's shape. Each plugin then checks its
own options (unknown keys, missing required keys, types). Cross-node rules (duplicate IDs, unknown
dependencies, kind mismatches, cycles) are checked in code.
"""
import graphlib
import json
from dataclasses import dataclass
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

from ingest.plugins import REGISTRY

SCHEMA = json.loads((Path(__file__).resolve().parent.parent / "schema" / "pipeline.schema.json").read_text())


class ConfigError(ValueError):
    pass


@dataclass
class Pipeline:
    name: str
    version: int
    nodes: dict      # id -> node spec (dict with id, kind, type, depends_on, options)
    order: list      # topological order, dependencies first


def order_nodes(deps):
    """Topological order of {node: [dependencies]}. Raises ConfigError on a cycle."""
    try:
        return list(graphlib.TopologicalSorter(deps).static_order())
    except graphlib.CycleError as e:
        raise ConfigError(f"cycle: {e.args[1]}") from None


def load_text(text):
    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ConfigError(f"not valid YAML: {str(e).splitlines()[0]}") from None
    if not isinstance(doc, dict):
        raise ConfigError("document must be a mapping with a 'pipeline' key")
    errors = sorted(Draft202012Validator(SCHEMA).iter_errors(doc), key=lambda e: list(e.path))
    if errors:
        raise ConfigError("; ".join(f"{'/'.join(map(str, e.path)) or '<root>'}: {e.message}" for e in errors))
    pipe = doc["pipeline"]
    nodes = {}
    for spec in pipe["nodes"]:
        nid = spec["id"]
        if nid in nodes:
            raise ConfigError(f"duplicate node id {nid!r}")
        plugin = REGISTRY.get(spec["type"])
        if plugin is None:
            raise ConfigError(f"{nid}: unknown type {spec['type']!r}")
        if plugin.KIND != spec["kind"]:
            raise ConfigError(f"{nid}: type {spec['type']!r} is a {plugin.KIND}, not a {spec['kind']}")
        deps = spec.get("depends_on", [])
        if spec["kind"] == "source" and deps:
            raise ConfigError(f"{nid}: a source cannot depend on other nodes")
        if spec["kind"] != "source" and not deps:
            raise ConfigError(f"{nid}: a {spec['kind']} needs at least one depends_on entry")
        try:
            plugin.check_options(spec.get("options", {}))
        except ValueError as e:
            raise ConfigError(f"{nid}: {e}") from None
        nodes[nid] = spec
    for nid, spec in nodes.items():
        for d in spec.get("depends_on", []):
            if d not in nodes:
                raise ConfigError(f"{nid}: depends on unknown node {d!r}")
    order = order_nodes({nid: spec.get("depends_on", []) for nid, spec in nodes.items()})
    return Pipeline(pipe["name"], pipe["version"], nodes, order)


def load(path):
    return load_text(Path(path).read_text())
