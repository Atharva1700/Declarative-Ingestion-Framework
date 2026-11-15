"""Independent pure-Python recomputation of the pipeline's output, for checking the Spark result.

Same rules as SQL_DEDUPE and SQL_ENRICH: keep the highest offset per event_id, drop non-positive
amounts, left-join accounts and tax rates, tax amount = truncated amount * (10000 + bps) / 10000.
"""
import json

import psycopg
import pyarrow as pa

from ingest.checksum import table_checksum


def expected_table(events_path, pg_dsn, rate_rows):
    """rate_rows: iterable of (region, tax_bps as str). Returns the expected arrow table."""
    latest = {}
    with open(events_path) as f:
        for off, line in enumerate(f):
            r = json.loads(line)
            cur = latest.get(r["event_id"])
            if cur is None or off > cur[0]:
                latest[r["event_id"]] = (off, r)
    with psycopg.connect(pg_dsn) as c:
        acct_region = dict(c.execute("SELECT account, region FROM accounts").fetchall())
    tax = {r: int(b) for r, b in rate_rows}
    cols = {k: [] for k in ("event_id", "account", "region", "amount_cents", "amount_with_tax_cents", "event_ts")}
    for eid, (_, r) in latest.items():
        if r["amount_cents"] <= 0:
            continue
        region = acct_region.get(r["account"])
        bps = tax.get(region, 0) if region is not None else 0
        vals = (eid, r["account"], region, r["amount_cents"],
                int(r["amount_cents"] * (10000 + bps) / 10000), r["event_ts"])
        for k, v in zip(cols, vals):
            cols[k].append(v)
    return pa.table(cols)


def expected_checksum(events_path, pg_dsn, rate_rows):
    return table_checksum(expected_table(events_path, pg_dsn, rate_rows))
