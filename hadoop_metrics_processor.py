#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hadoop metrics and logs processor
=================================

This optional job demonstrates how to consume Hadoop metrics and logs that
have been ingested into Elasticsearch by Elastic Agent.  It uses the
`elasticsearch‑hadoop` connector to read one or more data streams, performs
basic aggregations, and optionally writes the summary results back to
Elasticsearch.

Elastic’s Hadoop integration collects metrics from the Resource Manager API
and JMX API, covering application, cluster, DataNode, NameNode and
NodeManager metrics【766323546998156†L2-L12】.  The data are stored in
time‑series data streams such as `metrics-hadoop.*` for metrics and
`logs-hadoop.*` for logs.  This script illustrates how you might use
Spark to interrogate those data streams.

Example usage:

    spark-submit \
      --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.13.2 \
      hadoop_metrics_processor.py \
        --es-nodes es01.example.com \
        --metrics-index metrics-hadoop.* \
        --logs-index logs-hadoop.* \
        --output-index hadoop-metrics-summary

The script will compute average memory and vcore allocation per host across
all applications and write the results to the specified output index.  If
`--output-index` is omitted, the summary will be printed to stdout.

You can extend this script to perform more sophisticated analyses, join
metrics with audit logs (for example by `host.name` or timestamp), or
correlate log events with cluster resource usage.
"""

import argparse
import os
import sys
from typing import Iterable, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, col


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process Hadoop metrics and logs collected by Elastic Agent"
    )
    parser.add_argument(
        "--es-nodes",
        dest="es_nodes",
        required=True,
        help="Comma‑separated list of Elasticsearch hostnames",
    )
    parser.add_argument(
        "--es-port",
        dest="es_port",
        default="9200",
        help="Elasticsearch port (default 9200)",
    )
    parser.add_argument(
        "--metrics-index",
        dest="metrics_index",
        default="metrics-hadoop.*",
        help="Elasticsearch index pattern for Hadoop metrics (default: metrics-hadoop.*)",
    )
    parser.add_argument(
        "--logs-index",
        dest="logs_index",
        default=None,
        help="Elasticsearch index pattern for Hadoop logs (optional)",
    )
    parser.add_argument(
        "--output-index",
        dest="output_index",
        default=None,
        help=(
            "If specified, the aggregated results will be written to this Elasticsearch index; "
            "otherwise they will be displayed on stdout."
        ),
    )
    parser.add_argument(
        "--es-username",
        dest="es_username",
        default=None,
        help="Elasticsearch username for basic auth (optional)",
    )
    parser.add_argument(
        "--es-password",
        dest="es_password",
        default=None,
        help="Elasticsearch password for basic auth (optional)",
    )
    return parser.parse_args(argv)


def read_es(spark: SparkSession, index_pattern: str, options: dict) -> DataFrame:
    """Helper to read an index or index pattern from Elasticsearch."""
    reader = spark.read.format("es")
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load(index_pattern)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)

    spark = (
        SparkSession.builder
        .appName("HadoopMetricsProcessor")
        .config("es.nodes", args.es_nodes)
        .config("es.port", args.es_port)
        .config("es.index.read.missing.as.empty", "true")
        .getOrCreate()
    )

    es_read_options = {
        "es.nodes": args.es_nodes,
        "es.port": args.es_port,
        "es.nodes.wan.only": "false",
    }
    es_write_options = {
        "es.nodes": args.es_nodes,
        "es.port": args.es_port,
    }
    if args.es_username and args.es_password:
        es_read_options["es.net.http.auth.user"] = args.es_username
        es_read_options["es.net.http.auth.pass"] = args.es_password
        es_write_options.update(es_read_options)

    # Load metrics from Elasticsearch.  The structure of each document depends on
    # the specific dataset; we assume ECS fields such as host.name exist.
    metrics_df = read_es(spark, args.metrics_index, es_read_options)

    if metrics_df.rdd.isEmpty():
        print(f"No documents found in {args.metrics_index}. Exiting.")
        spark.stop()
        return

    # Example aggregation: compute average allocated memory (MB) and vcores per host.
    # Not all metrics documents will contain these fields; Spark will ignore missing
    # values in the aggregation.
    # Use getField = col('hadoop.application.allocated.mb') etc.
    mb_col = col("hadoop.application.allocated.mb")
    vcore_col = col("hadoop.application.allocated.v_cores")
    host_col = col("host.name")

    summary_df = (
        metrics_df
        .groupBy(host_col.alias("host"))
        .agg(
            avg(mb_col).alias("avg_allocated_mb"),
            avg(vcore_col).alias("avg_allocated_vcores"),
        )
        .orderBy("host")
    )

    # If logs_index is provided, you could join log events here.  As a simple
    # illustration we load the logs and count the number of log events per host.
    if args.logs_index:
        logs_df = read_es(spark, args.logs_index, es_read_options)
        if not logs_df.rdd.isEmpty():
            log_counts = logs_df.groupBy(col("host.name").alias("host")).count().alias("log_count")
            summary_df = summary_df.join(log_counts, on="host", how="left")

    # Write or display the results.
    if args.output_index:
        writer = (
            summary_df
            .write
            .format("es")
            .mode("append")
            .option("es.resource", args.output_index)
        )
        for k, v in es_write_options.items():
            writer = writer.option(k, v)
        writer.save()
        print(f"Summary written to index {args.output_index}")
    else:
        summary_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])