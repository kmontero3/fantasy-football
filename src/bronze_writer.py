import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)


def utc_now():
    return datetime.now(timezone.utc)


def write_raw(
    spark,
    catalog,
    schema,
    table,
    records,
    season=None,
    league_id=None,
    endpoint=None,
    run_id=None,
):
    retrieved_at = utc_now()

    bronze_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("retrieved_at", TimestampType(), False),
        StructField("season", IntegerType(), True),
        StructField("league_id", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("record_json", StringType(), False),
    ])

    rows = [
        (
            run_id,
            retrieved_at,
            season,
            league_id,
            endpoint,
            json.dumps(record),
        )
        for record in records
    ]

    df = spark.createDataFrame(rows, schema=bronze_schema)

    full_name = f"{catalog}.{schema}.{table}"

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(full_name)
    )
