#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PySpark job to ingest Apache Ranger audit logs from S3, enrich them with
database and table names using a mapping file, and index the results into
Elasticsearch.  The job assumes logs are stored as JSON and uses the
`elasticsearch‑hadoop` connector for writing to Elasticsearch.

Example usage:

```
spark‑submit \
  --packages org.elasticsearch:elasticsearch‑spark‑30_2.12:8.13.2,org.apache.hadoop:hadoop‑aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key=<AWS_ACCESS_KEY_ID> \
  --conf spark.hadoop.fs.s3a.secret.key=<AWS_SECRET_ACCESS_KEY> \
  ranger_ingest.py \
    --s3‑audit‑path s3://bucket/ranger/audit/2025/07/26/ \
    --mapping‑path s3://bucket/config/table_mapping.csv \
    --es‑nodes es01.example.com,es02.example.com \
    --es‑index ranger‑audit \
    --es‑username elastic \
    --es‑password <PASSWORD>
```
"""

import argparse
import os
import sys
from typing import Dict, Iterable, Tuple, Optional

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, coalesce, lit, udf, struct
from pyspark.sql.types import StructType, StructField, StringType


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    """Parse command‑line arguments for the ingestion job."""
    parser = argparse.ArgumentParser(
        description=(
            "Ingest Ranger audit logs from S3, enrich them with DB and table names, "
            "and write to Elasticsearch."
        )
    )
    parser.add_argument(
        "--s3-audit-path",
        dest="s3_audit_path",
        required=True,
        help="S3 URI or glob pattern pointing to Ranger audit log files (JSON).",
    )
    parser.add_argument(
        "--mapping-path",
        dest="mapping_path",
        required=True,
        help=(
            "Path to a CSV file containing columns 'table_path', 'db_name' and 'table_name'. "
            "This file may reside in S3 or on the local filesystem."
        ),
    )
    parser.add_argument(
        "--es-nodes",
        dest="es_nodes",
        required=True,
        help="Comma‑separated list of Elasticsearch hostnames.",
    )
    parser.add_argument(
        "--es-port",
        dest="es_port",
        default="9200",
        help="Elasticsearch port (default: 9200).",
    )
    parser.add_argument(
        "--es-index",
        dest="es_index",
        required=True,
        help="Elasticsearch index name where enriched logs will be written.",
    )
    parser.add_argument(
        "--path-column",
        dest="path_column",
        default="resource_path",
        help=(
            "Name of the field in the audit log containing the HDFS path. "
            "If this column is absent, the script attempts to extract 'path' from a nested 'resource' object."
        ),
    )
    parser.add_argument(
        "--aws-access-key-id",
        dest="aws_access_key_id",
        default=os.environ.get("AWS_ACCESS_KEY_ID"),
        help=(
            "AWS access key for reading from S3.  Defaults to the 'AWS_ACCESS_KEY_ID' environment variable."
        ),
    )
    parser.add_argument(
        "--aws-secret-access-key",
        dest="aws_secret_access_key",
        default=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        help=(
            "AWS secret key for reading from S3.  Defaults to the 'AWS_SECRET_ACCESS_KEY' environment variable."
        ),
    )
    parser.add_argument(
        "--es-username",
        dest="es_username",
        default=None,
        help="Username for basic authentication with Elasticsearch (optional).",
    )
    parser.add_argument(
        "--es-password",
        dest="es_password",
        default=None,
        help="Password for basic authentication with Elasticsearch (optional).",
    )
    return parser.parse_args(argv)


def broadcast_mapping(spark: SparkSession, mapping_path: str) -> Dict[str, Tuple[str, str]]:
    """Read the mapping CSV into a dictionary keyed by table_path and broadcast it."""
    # Load mapping CSV.  We infer the schema and require a header row.
    mapping_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(mapping_path)
    )
    required_cols = {"table_path", "db_name", "table_name"}
    missing = required_cols.difference({c.lower() for c in mapping_df.columns})
    if missing:
        raise ValueError(
            f"Mapping file is missing required columns: {', '.join(sorted(missing))}"
        )
    # Normalize column names (lowercase) to allow case‑insensitive headers.
    for col_name in mapping_df.columns:
        mapping_df = mapping_df.withColumnRenamed(col_name, col_name.lower())

    # Collect rows to the driver.  We assume the mapping file is small enough to fit in memory.
    rows = mapping_df.select("table_path", "db_name", "table_name").collect()
    path_map: Dict[str, Tuple[str, str]] = {}
    for row in rows:
        table_path = row["table_path"].rstrip("/") if row["table_path"] else ""
        path_map[table_path] = (row["db_name"], row["table_name"])
    return path_map


def enrich_logs(
    spark: SparkSession,
    logs_df: DataFrame,
    path_map: Dict[str, Tuple[str, str]],
    path_column: str,
) -> DataFrame:
    """Enrich the audit logs with database and table names using a broadcast mapping."""
    # Broadcast the mapping so all executors can access it efficiently.
    broadcast_map = spark.sparkContext.broadcast(path_map)

    # Define UDF to look up the database and table for a given HDFS path.
    def get_db_table(path: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if path is None:
            return (None, None)
        # Remove trailing slash to normalise the path.
        norm_path = path.rstrip("/")
        # Iterate over mapping entries and return the first match whose table_path is a prefix.
        for table_path, (db, table) in broadcast_map.value.items():
            if table_path and norm_path.startswith(table_path):
                return (db, table)
        return (None, None)

    schema = StructType([
        StructField("db", StringType(), True),
        StructField("table", StringType(), True),
    ])
    get_db_table_udf = udf(get_db_table, schema)

    # Attempt to create the path column if it does not exist.  We try to extract 'path'
    # from a nested 'resource' column (as used by Ranger) when possible.
    if path_column not in logs_df.columns:
        if "resource" in logs_df.columns:
            # Use dot notation to access a sub‑field within a struct.  If the field does not exist
            # the result will be null.  We avoid catching AnalysisException directly so that
            # Spark handles unknown fields gracefully.
            logs_df = logs_df.withColumn(path_column, col("resource.path"))
        else:
            # Create a null column to ensure the schema contains the path column.
            logs_df = logs_df.withColumn(path_column, lit(None).cast(StringType()))

    # Apply the UDF to compute the db and table names based on the path column.
    enriched = logs_df.withColumn(
        "_db_table", get_db_table_udf(col(path_column))
    )
    enriched = enriched.withColumn("db_name_mapped", col("_db_table.db"))
    enriched = enriched.withColumn("table_name_mapped", col("_db_table.table"))
    enriched = enriched.drop("_db_table")

    # Derive existing db and table columns from known column names if present.
    candidate_db_cols = [c for c in ["db_name", "database", "db", "dbname"] if c in enriched.columns]
    candidate_table_cols = [c for c in ["table_name", "table", "tbl", "tablename"] if c in enriched.columns]

    # Use coalesce to prioritise existing values over mapped values.
    if candidate_db_cols:
        enriched = enriched.withColumn(
            "db_name_combined", coalesce(*[col(c) for c in candidate_db_cols] + [col("db_name_mapped")])
        )
    else:
        enriched = enriched.withColumn(
            "db_name_combined", coalesce(col("db_name_mapped"), lit(None).cast(StringType()))
        )

    if candidate_table_cols:
        enriched = enriched.withColumn(
            "table_name_combined", coalesce(*[col(c) for c in candidate_table_cols] + [col("table_name_mapped")])
        )
    else:
        enriched = enriched.withColumn(
            "table_name_combined", coalesce(col("table_name_mapped"), lit(None).cast(StringType()))
        )

    # Replace or create the final db_name and table_name columns.
    enriched = enriched.withColumnRenamed("db_name_combined", "db_name")
    enriched = enriched.withColumnRenamed("table_name_combined", "table_name")

    # Drop intermediate columns.
    drop_cols = ["db_name_mapped", "table_name_mapped"]
    # Remove any duplicates from candidate lists to avoid errors.
    for col_name in set(candidate_db_cols + candidate_table_cols):
        if col_name in ["db_name", "table_name"]:
            # Skip final column names to avoid accidentally dropping the new columns.
            continue
        drop_cols.append(col_name)
    for c in drop_cols:
        if c in enriched.columns:
            enriched = enriched.drop(c)
    return enriched


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)

    # Build the SparkSession.  We do not specify `spark.jars.packages` here
    # because the correct version of elasticsearch‑hadoop should be provided
    # via the spark‑submit command.  See the README for an example.
    spark = (
        SparkSession.builder
        .appName("RangerAuditIngest")
        # Configure Elasticsearch connection properties.  These will be used
        # implicitly by the elasticsearch‑hadoop connector when writing.
        .config("es.nodes", args.es_nodes)
        .config("es.port", args.es_port)
        .config("es.index.auto.create", "true")
        .getOrCreate()
    )

    # Set AWS credentials for the S3A filesystem if provided.  Using
    # SimpleAWSCredentialsProvider ensures the credentials are passed directly
    # without relying on environment variables inside the executors.
    if args.aws_access_key_id and args.aws_secret_access_key:
        spark._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        spark._jsc.hadoopConfiguration().set(
            "fs.s3a.access.key", args.aws_access_key_id
        )
        spark._jsc.hadoopConfiguration().set(
            "fs.s3a.secret.key", args.aws_secret_access_key
        )
    # Otherwise the Hadoop S3A connector will follow the default provider chain (IAM role, etc.).

    # Set basic authentication for Elasticsearch if provided.
    es_write_options = {}
    if args.es_username and args.es_password:
        es_write_options["es.net.http.auth.user"] = args.es_username
        es_write_options["es.net.http.auth.pass"] = args.es_password

    # Read the mapping file and broadcast it.
    path_map = broadcast_mapping(spark, args.mapping_path)

    # Read the Ranger audit logs from S3.  The result is a DataFrame of nested JSON rows.
    logs_df = spark.read.json(args.s3_audit_path)

    # Enrich the logs with db and table names.
    enriched_df = enrich_logs(spark, logs_df, path_map, args.path_column)

    # Write the enriched events to Elasticsearch.  Use append mode so that existing
    # documents in the index are preserved.
    writer = (
        enriched_df.write
        .format("es")
        .mode("append")
        .option("es.resource", args.es_index)
    )
    # Apply authentication options if necessary.
    for k, v in es_write_options.items():
        writer = writer.option(k, v)
    writer.save()

    spark.stop()


if __name__ == "__main__":
    # Pass through argv unmodified so that argparse can see CLI arguments.
    main(sys.argv[1:])