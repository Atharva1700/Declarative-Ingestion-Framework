"""Run a compiled pipeline in topological order and record the run in PostgreSQL."""
import time
import uuid
from dataclasses import dataclass

import psycopg

from ingest.config import load
from ingest.plugins import REGISTRY, Context

STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id text PRIMARY KEY, pipeline text NOT NULL, status text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
    row_count bigint, checksum text, error text);
"""


@dataclass
class RunResult:
    run_id: str
    status: str
    row_count: int
    checksum: str
    seconds: float


def run(config_path, spark, pg_dsn, warehouse):
    pipe = load(config_path)
    run_id = str(uuid.uuid4())
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        c.execute(STATE_SCHEMA)
        c.execute("INSERT INTO ingest_runs (run_id, pipeline, status) VALUES (%s, %s, 'running')",
                  (run_id, pipe.name))
    ctx = Context(spark, warehouse)
    frames, sink_id = {}, None
    t0 = time.perf_counter()
    try:
        for nid in pipe.order:
            node = pipe.nodes[nid]
            plugin = REGISTRY[node["type"]]()
            inputs = {d: frames[d] for d in node.get("depends_on", [])}
            out = plugin.execute(ctx, node, inputs)
            if out is not None:
                frames[nid] = out
            if node["kind"] == "sink":
                sink_id = nid
        checksum, rows = ctx.sink_checksums[sink_id]
        status, error = "ok", None
    except Exception as e:  # recorded, then re-raised
        checksum, rows, status, error = None, None, "failed", repr(e)[:500]
        raise
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as c:
            c.execute("UPDATE ingest_runs SET status=%s, finished_at=now(), row_count=%s, checksum=%s, error=%s "
                      "WHERE run_id=%s", (status, rows, checksum, error, run_id))
    return RunResult(run_id, status, rows, checksum, round(time.perf_counter() - t0, 3))
