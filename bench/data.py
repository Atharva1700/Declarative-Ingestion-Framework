"""Synthetic inputs and the pipeline config. Seeded, so the same seed gives the same files."""
import json
import random
from pathlib import Path

import psycopg
import yaml

REGIONS = [f"R{i:02d}" for i in range(12)]

SQL_DEDUPE = """
SELECT event_id, account, amount_cents, event_ts FROM (
  SELECT events.*, row_number() OVER (PARTITION BY event_id ORDER BY src_offset DESC) AS rn FROM events
) WHERE rn = 1 AND amount_cents > 0
"""
SQL_ENRICH = """
SELECT d.event_id, d.account, a.region, d.amount_cents,
       CAST(d.amount_cents * (10000 + COALESCE(CAST(t.tax_bps AS INT), 0)) / 10000 AS BIGINT) AS amount_with_tax_cents,
       d.event_ts
FROM dedupe d
LEFT JOIN accounts a ON d.account = a.account
LEFT JOIN tax_rates t ON a.region = t.region
"""


def make_config(data_dir, pg_dsn):
    d = Path(data_dir)
    doc = {"pipeline": {"name": "enriched_events", "version": 1, "nodes": [
        {"id": "events", "kind": "source", "type": "kafka_file", "options": {"path": str(d / "topic_events.jsonl")}},
        {"id": "accounts", "kind": "source", "type": "jdbc_postgres",
         "options": {"dsn": pg_dsn, "query": "SELECT account, region FROM accounts"}},
        {"id": "tax_rates", "kind": "source", "type": "s3_files", "options": {"path": str(d / "rates")}},
        {"id": "dedupe", "kind": "transform", "type": "spark_sql", "depends_on": ["events"],
         "options": {"sql": SQL_DEDUPE}},
        {"id": "enriched", "kind": "transform", "type": "spark_sql",
         "depends_on": ["dedupe", "accounts", "tax_rates"], "options": {"sql": SQL_ENRICH}},
        {"id": "warehouse_sink", "kind": "sink", "type": "iceberg", "depends_on": ["enriched"],
         "options": {"table": "ingest.enriched_events"}},
    ]}}
    path = d / "pipeline.yaml"
    path.write_text(yaml.safe_dump(doc, sort_keys=False))
    return path


def generate(data_dir, pg_dsn, n_events=200_000, n_accounts=20_000, seed=1):
    """Topic with duplicates (later offset wins) and some non-positive amounts; accounts in PG; tax CSVs."""
    rng = random.Random(seed)
    d = Path(data_dir)
    (d / "rates").mkdir(parents=True, exist_ok=True)
    accounts = [(f"acc{i:06d}", rng.choice(REGIONS)) for i in range(n_accounts)]
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS accounts")
        c.execute("CREATE TABLE accounts (account text PRIMARY KEY, region text NOT NULL)")
        with c.cursor() as cur:
            cur.executemany("INSERT INTO accounts VALUES (%s, %s)", accounts)
    for k in range(3):
        with open(d / "rates" / f"part-{k}.csv", "w") as f:
            f.write("region,tax_bps\n")
            for i, r in enumerate(REGIONS):
                if i % 3 == k:
                    f.write(f"{r},{rng.randint(0, 2000)}\n")
    with open(d / "topic_events.jsonl", "w") as f:
        ids = list(range(1, n_events + 1))
        rng.shuffle(ids)
        for i in range(n_events):
            eid = ids[i] if i < n_events * 0.9 else rng.randint(1, n_events)  # ~10% re-sent event_ids
            f.write(json.dumps({"event_id": eid, "account": f"acc{rng.randrange(n_accounts):06d}",
                                "amount_cents": rng.randint(-500, 50_000),
                                "event_ts": f"2026-11-{1 + i % 28:02d}T00:00:00Z"}) + "\n")
    return d
