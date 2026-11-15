"""Deterministic table checksum: every row as canonical JSON, rows sorted on all columns."""
import hashlib
import json


def table_checksum(arrow_table):
    names = arrow_table.column_names
    rows = arrow_table.to_pylist()
    rows.sort(key=lambda r: tuple("" if r[n] is None else str(r[n]) for n in names))
    h = hashlib.sha256()
    for r in rows:
        h.update(json.dumps(r, sort_keys=True, default=str).encode())
        h.update(b"\n")
    return h.hexdigest()
