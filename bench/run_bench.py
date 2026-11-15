"""Scale run: 200k events, 20k accounts, three replays. Writes results/ingest.json.

Checks: each run's checksum equals the independent Python oracle; the three replays have equal
checksums; invalid-config and DAG checks from the test suite are counted again here.
"""
import json
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import psycopg  # noqa: E402
from bench.data import generate, make_config  # noqa: E402
from bench.oracle import expected_table  # noqa: E402
from ingest.checksum import table_checksum  # noqa: E402
from ingest.runner import run  # noqa: E402
from ingest.spark import get_spark  # noqa: E402
from ingest.config import ConfigError, load  # noqa: E402
from tests.test_ingest import INVALID, DSN, load_text  # noqa: E402
import random  # noqa: E402
from ingest.config import order_nodes  # noqa: E402


def main():
    d = Path(tempfile.mkdtemp(prefix="ing_bench_"))
    t0 = time.perf_counter()
    generate(d, DSN, n_events=200_000, n_accounts=20_000, seed=1)
    gen_s = round(time.perf_counter() - t0, 1)
    rates = []
    for f in sorted((d / "rates").glob("*.csv")):
        for line in f.read_text().splitlines()[1:]:
            r, b = line.split(",")
            rates.append((r, b))
    exp = expected_table(d / "topic_events.jsonl", DSN, rates)
    exp_sum = table_checksum(exp)
    cfg = make_config(d, DSN)
    spark = get_spark("bench")

    runs = []
    for i in range(3):
        r = run(cfg, spark, DSN, d / "wh")
        runs.append({"run": i + 1, "status": r.status, "rows": r.row_count, "seconds": r.seconds,
                     "checksum": r.checksum, "matches_oracle": r.checksum == exp_sum})

    with psycopg.connect(DSN) as c:
        statuses = [s for (s,) in c.execute("SELECT status FROM ingest_runs WHERE pipeline='enriched_events' "
                                            "ORDER BY started_at DESC LIMIT 3")]
    from pyiceberg.catalog.sql import SqlCatalog
    cat = SqlCatalog("ingest", uri=f"sqlite:///{d / 'wh' / 'catalog.db'}", warehouse=f"file://{d / 'wh'}")
    tbl = cat.load_table("ingest.enriched_events")
    unmatched = sum(1 for r in tbl.scan(row_filter="region IS NULL").to_arrow().to_pylist())

    invalid_rejected = 0
    for text in INVALID.values():
        try:
            load_text(text)
        except ConfigError:
            invalid_rejected += 1
    rng = random.Random(7)
    dag_ok = 0
    for _ in range(300):
        n = rng.randint(1, 25)
        deps = {i: [j for j in range(i) if rng.random() < 0.3] for i in range(n)}
        order = order_nodes(deps)
        pos = {v: k for k, v in enumerate(order)}
        dag_ok += all(pos[dd] < pos[i] for i, ds in deps.items() for dd in ds)

    with open(d / "topic_events.jsonl") as f:
        input_rows = sum(1 for _ in f)
    result = {
        "input": {"topic_events": input_rows, "accounts": 20_000, "tax_rate_rows": len(rates),
                  "generate_seconds": gen_s},
        "output": {"rows": exp.num_rows, "rows_with_unmatched_account_region": unmatched,
                   "expected_checksum": exp_sum},
        "runs": runs,
        "run_statuses_recorded": statuses,
        "replays_identical": len({r["checksum"] for r in runs}) == 1,
        "iceberg_snapshots_after_3_runs": len(tbl.metadata.snapshots),
        "invalid_configs_rejected": f"{invalid_rejected}/{len(INVALID)}",
        "random_dags_correctly_ordered": f"{dag_ok}/300",
    }
    out = ROOT / "results" / "ingest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
