"""Plugins: one class per source, transform or sink type. Adding a type means adding a class.

Substitutions (no Maven or Docker here, see README):
- kafka_file stands in for a Kafka topic: a JSONL file, offset = line number.
- jdbc_postgres uses psycopg, not Spark JDBC (no JDBC driver jar is available).
- s3_files reads a local directory with Spark's CSV reader, not s3a:// (no MinIO).
- iceberg sink writes a real Iceberg table through pyiceberg (SQL catalog, local warehouse).
"""
import json
from pathlib import Path

import psycopg
from pyiceberg.catalog.sql import SqlCatalog
from pyspark.sql.types import LongType, StringType, StructField, StructType

REGISTRY = {}

EVENT_SCHEMA = StructType([
    StructField("event_id", LongType(), False),
    StructField("account", StringType(), False),
    StructField("amount_cents", LongType(), False),
    StructField("event_ts", StringType(), False),
    StructField("src_offset", LongType(), False),
])


def register(cls):
    REGISTRY[cls.TYPE] = cls
    return cls


class Plugin:
    KIND = ""
    TYPE = ""
    OPTIONS = {}   # name -> (python type, required)

    @classmethod
    def check_options(cls, opts):
        for k in opts:
            if k not in cls.OPTIONS:
                raise ValueError(f"{cls.TYPE}: unknown option {k!r}")
        for k, (typ, required) in cls.OPTIONS.items():
            if k not in opts:
                if required:
                    raise ValueError(f"{cls.TYPE}: missing option {k!r}")
                continue
            if not isinstance(opts[k], typ):
                raise ValueError(f"{cls.TYPE}.{k}: expected {typ.__name__}")


class Context:
    def __init__(self, spark, warehouse):
        self.spark = spark
        self.warehouse = Path(warehouse)
        self.sink_checksums = {}


@register
class KafkaFileSource(Plugin):
    KIND, TYPE = "source", "kafka_file"
    OPTIONS = {"path": (str, True)}

    def execute(self, ctx, node, inputs):
        rows = []
        with open(node["options"]["path"]) as f:
            for offset, line in enumerate(f):
                r = json.loads(line)
                rows.append((r["event_id"], r["account"], r["amount_cents"], r["event_ts"], offset))
        return ctx.spark.createDataFrame(rows, EVENT_SCHEMA)


@register
class JdbcPostgresSource(Plugin):
    KIND, TYPE = "source", "jdbc_postgres"
    OPTIONS = {"dsn": (str, True), "query": (str, True)}

    def execute(self, ctx, node, inputs):
        with psycopg.connect(node["options"]["dsn"]) as c:
            cur = c.execute(node["options"]["query"])
            names = [d.name for d in cur.description]
            rows = [tuple(r) for r in cur.fetchall()]
        return ctx.spark.createDataFrame(rows, names)


@register
class S3FilesSource(Plugin):
    KIND, TYPE = "source", "s3_files"
    OPTIONS = {"path": (str, True)}

    def execute(self, ctx, node, inputs):
        return ctx.spark.read.option("header", "true").csv(node["options"]["path"])


@register
class SparkSqlTransform(Plugin):
    """Each depends_on input is exposed as a temp view with its node id as the name."""
    KIND, TYPE = "transform", "spark_sql"
    OPTIONS = {"sql": (str, True)}

    def execute(self, ctx, node, inputs):
        for name, df in inputs.items():
            df.createOrReplaceTempView(name)
        return ctx.spark.sql(node["options"]["sql"])


@register
class IcebergSink(Plugin):
    KIND, TYPE = "sink", "iceberg"
    OPTIONS = {"table": (str, True)}   # "namespace.name"

    def execute(self, ctx, node, inputs):
        from pyiceberg.exceptions import NamespaceAlreadyExistsError
        (df,) = inputs.values()
        arrow = df.toArrow()
        ns, name = node["options"]["table"].split(".")
        warehouse = ctx.warehouse
        warehouse.mkdir(parents=True, exist_ok=True)
        catalog = SqlCatalog("ingest", uri=f"sqlite:///{warehouse / 'catalog.db'}",
                             warehouse=f"file://{warehouse}")
        try:
            catalog.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass
        table = catalog.create_table_if_not_exists(f"{ns}.{name}", schema=arrow.schema)
        table.overwrite(arrow)   # full snapshot: a replay of the same inputs writes the same rows
        back = catalog.load_table(f"{ns}.{name}").scan().to_arrow()
        from ingest.checksum import table_checksum
        ctx.sink_checksums[node["id"]] = (table_checksum(back), back.num_rows)
        return None
