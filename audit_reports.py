#!/usr/bin/env python3
"""
PySpark utility to generate sample audit reports from enriched Ranger audit logs
stored in Elasticsearch.  The reports answer questions like "who is accessing
what" by aggregating events by user, database and table.  The script reads
events from an Elasticsearch index via the elasticsearch-hadoop connector,
computes several summary tables, and writes them to disk as CSV files or
displays them on stdout.

Example usage:

```
spark-submit \
  --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.13.2 \
  audit_reports.py \
    --es-nodes es01.example.com,es02.example.com \
    --es-index ranger-audit \
    --output-dir file:///tmp/audit-reports \
    --es-username elastic \
    --es-password <PASSWORD>
```

The script writes three CSV files into the specified output directory:

* `user_resource_summary.csv`: number of accesses, earliest and latest
  event times per user, database and table.
* `top_users.csv`: top users ranked by total number of events across all
  resources.
* `resource_access_frequency.csv`: counts of events per database and table.

If no `--output-dir` is specified the summaries will be shown on the console
instead of being written to disk.
"""

import argparse
import os
import sys
from typing import Iterable, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, min as spark_min, max as spark_max


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    """Parse command‑line arguments for the report generator."""
    parser = argparse.ArgumentParser(
        description=(
            "Generate sample audit reports from enriched Ranger audit logs in Elasticsearch."
        )
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
        help="Name of the Elasticsearch index containing enriched audit events.",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default=None,
        help=(
            "Filesystem path or URI where CSV reports will be written.  If omitted, "
            "the summaries are printed to the console instead of being saved."
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

    parser.add_argument(
        "--top-n",
        dest="top_n",
        type=int,
        default=10,
        help=(
            "Number of top users to return in the top users report (default: 10)."
        ),
    )
    return parser.parse_args(argv)


def load_events(spark: SparkSession, index: str) -> DataFrame:
    """Load enriched audit events from an Elasticsearch index into a DataFrame."""
    return spark.read.format("es").load(index)


def choose_event_time_column(df: DataFrame) -> str:
    """
    Choose a column to represent the event time.  Ranger audit logs may use
    different field names (e.g., `eventTime`, `evtTime`, `access_time`).  This
    helper selects the first matching column present in the DataFrame.
    """
    candidates = ["eventTime", "evtTime", "access_time", "event_time"]
    for c in candidates:
        if c in df.columns:
            return c
    # Fall back to Spark's internal timestamp if none are available.
    return None


def compute_user_resource_summary(df: DataFrame) -> DataFrame:
    """
    Compute a summary of accesses per user and resource (db/table).  The summary
    includes the number of accesses along with the earliest and latest event
    times for each grouping.
    """
    time_col = choose_event_time_column(df)
    group_cols = ["user", "db_name", "table_name"]
    agg_exprs = [count("*").alias("access_count")]
    if time_col:
        agg_exprs.append(spark_min(col(time_col)).alias("first_event"))
        agg_exprs.append(spark_max(col(time_col)).alias("last_event"))
    return df.groupBy(*group_cols).agg(*agg_exprs)


def compute_top_users(df: DataFrame, top_n: int = 10) -> DataFrame:
    """Compute the top N users by number of events across all resources."""
    return (
        df.groupBy("user")
        .agg(count("*").alias("total_events"))
        .orderBy(col("total_events").desc())
        .limit(top_n)
    )


def compute_resource_access_frequency(df: DataFrame) -> DataFrame:
    """Compute the number of events for each database and table."""
    return (
        df.groupBy("db_name", "table_name")
        .agg(count("*").alias("access_count"))
        .orderBy(col("access_count").desc())
    )


def save_or_show(df: DataFrame, path: Optional[str], filename: str) -> None:
    """Save the DataFrame as a CSV file or show it on the console."""
    if path:
        # Write to a single CSV file for ease of use.  We repartition to one
        # partition to ensure only one file is produced.
        output_path = os.path.join(path, filename)
        (
            df.repartition(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(output_path)
        )
        print(f"Saved report to {output_path}")
    else:
        df.show(truncate=False)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)

    # Build the SparkSession with Elasticsearch connection properties.
    spark = (
        SparkSession.builder
        .appName("RangerAuditReports")
        .config("es.nodes", args.es_nodes)
        .config("es.port", args.es_port)
        .config("es.nodes.wan.only", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # Set basic authentication for Elasticsearch if provided.
    if args.es_username and args.es_password:
        spark.conf.set("es.net.http.auth.user", args.es_username)
        spark.conf.set("es.net.http.auth.pass", args.es_password)

    # Load enriched audit events.
    events_df = load_events(spark, args.es_index)

    # Compute reports.
    user_resource_summary = compute_user_resource_summary(events_df)
    top_users = compute_top_users(events_df, args.top_n)
    resource_frequency = compute_resource_access_frequency(events_df)

    # Save or display reports.
    save_or_show(user_resource_summary, args.output_dir, "user_resource_summary")
    save_or_show(top_users, args.output_dir, "top_users")
    save_or_show(resource_frequency, args.output_dir, "resource_access_frequency")

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])