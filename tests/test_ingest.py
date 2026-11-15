"""Config validation, DAG ordering, checksums and an oracle check on the Spark output.
Run: python3 -m pytest -q tests/   (needs PostgreSQL on 127.0.0.1:5434, database ingest)
"""
import random
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from bench.data import generate, make_config  # noqa: E402
from bench.oracle import expected_checksum  # noqa: E402
from ingest.checksum import table_checksum  # noqa: E402
from ingest.config import ConfigError, load, load_text, order_nodes  # noqa: E402

DSN = "host=127.0.0.1 port=5434 dbname=ingest user=postgres"
GOOD = """
pipeline:
  name: demo_pipe
  version: 1
  nodes:
    - {id: src, kind: source, type: kafka_file, options: {path: /x.jsonl}}
    - {id: t1, kind: transform, type: spark_sql, depends_on: [src], options: {sql: "SELECT * FROM src"}}
    - {id: out, kind: sink, type: iceberg, depends_on: [t1], options: {table: ingest.demo}}
"""


def _replace(old, new):
    assert old in GOOD
    return GOOD.replace(old, new)


INVALID = {
    "missing_version": _replace("version: 1\n", ""),
    "bad_name": _replace("name: demo_pipe", "name: Demo-Pipe"),
    "extra_top_key": _replace("version: 1", "version: 1\n  owner: me"),
    "unknown_type": _replace("type: kafka_file", "type: kinesis"),
    "kind_mismatch": _replace("kind: source, type: kafka_file", "kind: sink, type: kafka_file"),
    "source_with_deps": _replace("{id: src, kind: source", "{id: src, depends_on: [out], kind: source"),
    "transform_without_deps": _replace("depends_on: [src], options: {sql", "options: {sql"),
    "unknown_dependency": _replace("depends_on: [t1]", "depends_on: [nope]"),
    "duplicate_id": _replace("{id: t1,", "{id: src,"),
    "cycle": _replace("{id: src, kind: source, type: kafka_file, options: {path: /x.jsonl}}",
                      "{id: src, kind: transform, type: spark_sql, depends_on: [out], options: {sql: 'x'}}"),
    "unknown_option": _replace("options: {path: /x.jsonl}", "options: {path: /x.jsonl, retries: 3}"),
    "missing_required_option": _replace("options: {table: ingest.demo}", "options: {}"),
    "wrong_option_type": _replace("options: {path: /x.jsonl}", "options: {path: 12}"),
}


def test_valid_config_loads_in_dependency_order():
    p = load_text(GOOD)
    assert p.order == ["src", "t1", "out"]


@pytest.mark.parametrize("name", sorted(INVALID))
def test_invalid_config_is_rejected(name):
    with pytest.raises(ConfigError):
        load_text(INVALID[name])


def test_random_dags_are_ordered_consistently():
    rng = random.Random(7)
    for _ in range(300):
        n = rng.randint(1, 25)
        deps = {i: [j for j in range(i) if rng.random() < 0.3] for i in range(n)}
        order = order_nodes(deps)
        pos = {v: k for k, v in enumerate(order)}
        assert sorted(order) == list(range(n))
        assert all(pos[d] < pos[i] for i, ds in deps.items() for d in ds)


def test_checksum_is_order_independent_and_sensitive():
    import pyarrow as pa
    t = pa.table({"a": [1, 2, 3], "b": ["x", None, "z"]})
    shuffled = t.take([2, 0, 1])
    assert table_checksum(t) == table_checksum(shuffled)
    changed = pa.table({"a": [1, 2, 3], "b": ["x", None, "y"]})
    assert table_checksum(t) != table_checksum(changed)


@pytest.fixture(scope="module")
def small_inputs():
    d = tempfile.mkdtemp(prefix="ing_test_")
    generate(d, DSN, n_events=3000, n_accounts=300, seed=3)
    return d, make_config(d, DSN)


def test_spark_output_matches_independent_oracle(small_inputs):
    from ingest.spark import get_spark
    from ingest.runner import run
    d, cfg = small_inputs
    spark = get_spark("test")
    res = run(cfg, spark, DSN, Path(d) / "wh")
    rates = []
    for f in sorted((Path(d) / "rates").glob("*.csv")):
        for line in f.read_text().splitlines()[1:]:
            r, b = line.split(",")
            rates.append((r, b))
    assert res.status == "ok"
    assert res.checksum == expected_checksum(Path(d) / "topic_events.jsonl", DSN, rates)
    assert res.row_count > 0


def test_replay_gives_identical_checksum(small_inputs):
    from ingest.spark import get_spark
    from ingest.runner import run
    d, cfg = small_inputs
    spark = get_spark("test")
    a = run(cfg, spark, DSN, Path(d) / "wh")
    b = run(cfg, spark, DSN, Path(d) / "wh")
    assert a.checksum == b.checksum and a.row_count == b.row_count


def test_failed_run_is_recorded(small_inputs, tmp_path):
    from ingest.spark import get_spark
    from ingest.runner import run
    d, _ = small_inputs
    bad = make_config(d, DSN).read_text().replace(
        "/rates", "/no/such/dir")
    bad_path = tmp_path / "bad.yaml"
    bad_path.write_text(bad)
    with pytest.raises(Exception):
        run(bad_path, get_spark("test"), DSN, Path(d) / "wh")
    import psycopg
    with psycopg.connect(DSN) as c:
        status = c.execute("SELECT status FROM ingest_runs ORDER BY started_at DESC LIMIT 1").fetchone()[0]
    assert status == "failed"
