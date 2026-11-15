# Declarative Ingestion Framework

A pipeline is a YAML file. It is checked against a JSON Schema and per-plugin option rules, compiled
to a topologically ordered DAG, and run on PySpark. Sources, transforms and sinks are plugins.
Every run records its status and a deterministic checksum of the output in PostgreSQL. The output
is written as a real Iceberg table.

## What runs and what is substituted

| Brief item | Here | Status |
|---|---|---|
| YAML config validated by JSON Schema | `schema/pipeline.schema.json` + jsonschema (Draft 2020-12) | Real |
| Topologically sorted DAG | stdlib `graphlib`; cycles, unknown or duplicate IDs, kind mismatches rejected | Real |
| Plugins (extensible) | `REGISTRY` + `@register`; each plugin validates its own options | Real |
| PySpark | `spark_sql` transform (Spark SQL over temp views), dedupe and join run in Spark 4.3 on local[2] | Real |
| Iceberg | `iceberg` sink via pyiceberg 0.12 (SQL catalog, local warehouse, table metadata and snapshots) | Real table format; **not** Spark-Iceberg (no runtime jar, see below) |
| PostgreSQL | run ledger (`ingest_runs`) and the JDBC-style source (accounts table) | Real PostgreSQL 16 |
| Kafka source | `kafka_file`: a JSONL file, offset = line number | **Substituted.** No broker; no Kafka connector jar |
| JDBC source | `jdbc_postgres` via psycopg into a Spark DataFrame | **Substituted.** Spark JDBC needs a driver jar from Maven, which the egress policy blocks |
| S3 source | `s3_files`: Spark CSV reader on a local directory | **Substituted.** No MinIO and no s3a jars |
| Deterministic replay checksums | SHA-256 over canonical JSON of every row, sorted on all columns, computed on the Iceberg table read back | Real |
| pytest | 19 tests | Real |

Maven Central returned 403 from the egress proxy, so no Spark connector jars (Iceberg runtime,
Kafka, JDBC) could be fetched. Writing to Iceberg through Spark was therefore not possible here.
The Iceberg tables are real Iceberg, written through pyiceberg. Nothing was routed around the block.

## Pipeline example

```yaml
pipeline:
  name: enriched_events
  version: 1
  nodes:
    - {id: events, kind: source, type: kafka_file, options: {path: data/topic_events.jsonl}}
    - {id: accounts, kind: source, type: jdbc_postgres, options: {dsn: "...", query: "SELECT account, region FROM accounts"}}
    - {id: tax_rates, kind: source, type: s3_files, options: {path: data/rates}}
    - {id: dedupe, kind: transform, type: spark_sql, depends_on: [events], options: {sql: "..."}}
    - {id: enriched, kind: transform, type: spark_sql, depends_on: [dedupe, accounts, tax_rates], options: {sql: "..."}}
    - {id: warehouse_sink, kind: sink, type: iceberg, depends_on: [enriched], options: {table: ingest.enriched_events}}
```

Full version: `bench/data.py` (`make_config`, SQL text).

## Results (`results/ingest.json`, 200k topic events, 20k accounts)

- **Output**: 180,048 rows. Duplicates resolved to the highest offset; non-positive amounts dropped.
- **Oracle agreement**: an independent pure-Python recomputation (`bench/oracle.py`) gives the same checksum as the pipeline for all three runs (`6fae4584…a5ac0b`). This checks the Spark SQL semantics, not only replay stability.
- **Replays**: three runs, identical checksums and row counts, all recorded `ok` in `ingest_runs`.
- **Timing**: 11.2 s for the first run (includes JVM and table creation), 4.7 s and 4.2 s for the next two, on 2 cores.
- **Config rejection**: 13 of 13 invalid configs rejected, covering missing fields, bad patterns, unknown types, kind mismatch, source with dependencies, transform without dependencies, unknown dependency, duplicate ID, cycle, unknown option, missing required option, wrong option type.
- **DAG ordering**: 300 of 300 random DAGs ordered with every edge respected.
- **Iceberg snapshots**: 5 after three runs. Each overwrite is a delete plus an append, so snapshot count is not one per run.

Tests (`python3 -m pytest -q tests/`, 19 pass): valid load, each invalid case, random DAGs, checksum
order-independence and sensitivity, Spark output versus oracle, replay equality, and a failed run
recorded as `failed` with the error.

## Bugs found while building (fixed)

- **PostgreSQL SQL_ASCII encoding.** The cluster's default database was SQL_ASCII, so psycopg returned `bytes` for text. Spark then typed the region column as binary and the oracle lookup missed every key. Fix: create the database as UTF-8. Caught by the oracle test.
- **Malformed YAML** raised `yaml.ScannerError` instead of `ConfigError`. Fixed in `load_text`. Caught by a test.
- **Reserved word.** `offset` is a Spark SQL keyword, so the column was renamed `src_offset`.

## Limits

- No real Kafka, MinIO, Spark-Iceberg or Spark JDBC. Each is substituted as listed above. Partition pruning, consumer groups and s3a credentials are not exercised.
- Full-refresh semantics: each run rewrites the whole table. There is no incremental merge, so this is not a CDC design.
- Options are checked; SQL inside `spark_sql` is not. A bad query fails at run time, and the failure is recorded.
- Plugin discovery is an in-process registry, not entry points.
- Throughput is for 2 local cores. It says nothing about cluster scale.

## Run

```
pip install -r requirements.txt
# PostgreSQL 16 on 127.0.0.1:5434, database `ingest` (UTF-8)
python3 -m pytest -q tests/
python3 -m bench.run_bench         # writes results/ingest.json
```
