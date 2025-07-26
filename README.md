# Ranger Audit Log Ingestion with PySpark

This project provides a simple PySpark job that reads [Apache Ranger](https://ranger.apache.org/) audit logs from an Amazon S3 bucket, enriches them with database and table information derived from a CSV mapping file, and writes the resulting events to an Elasticsearch index.  By attaching human‑readable database and table names to every HDFS access event, the job enables more meaningful audit reporting and analysis.

## Overview

Many audit events recorded by Ranger only carry the HDFS path that was accessed.  In order to correlate those paths with Hive databases and tables, you can maintain a CSV file containing three columns:

| Column      | Description                                      |
|-------------|--------------------------------------------------|
| `table_path`| Fully qualified HDFS path to the table location. |
| `db_name`   | The logical Hive database name.                  |
| `table_name`| The logical Hive table name.                     |

The ingestion job reads this mapping file, broadcasts it to all executors, and uses it to look up the correct `db_name` and `table_name` for each audit event whose path starts with the mapping’s `table_path`.  Any existing database or table names present in the audit record will take precedence, so that events generated directly by Hive or Impala retain their original metadata.

Audit events are assumed to be stored in JSON format within an S3 bucket.  You can supply a glob pattern (e.g. `s3://my‑bucket/ranger/audit/2025/07/26/*.json`) to read a subset of logs or a prefix (e.g. `s3://my‑bucket/ranger/audit/`) to ingest everything.  The job writes the enriched events to Elasticsearch using the [`elasticsearch‑hadoop`](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html) connector.

## Requirements

To run the job you will need the following:

* **Python 3.8+** and **PySpark 3.x** installed on your cluster.
* Access to the Ranger audit log files stored in S3.  The job uses the Hadoop `s3a` connector, so you must configure your AWS credentials either via command‑line arguments or environment variables.
* A running Elasticsearch cluster (version 7.x or 8.x) reachable from your Spark cluster.
* The `elasticsearch‑hadoop` connector packaged with your Spark job.  At runtime you specify the connector version in the `spark‑submit` command.

All Python dependencies are listed in `requirements.txt`.  Install them with

```bash
pip install -r requirements.txt
```

## Configuration

The ingestion job is configured entirely via command‑line arguments.  The key parameters are described below:

| Parameter                   | Default | Description |
|----------------------------|---------|-------------|
| `--s3‑audit‑path`          | _None_  | The S3 URI or glob pattern pointing to Ranger audit logs.  Required. |
| `--mapping‑path`           | _None_  | Path to the CSV mapping file (can be S3 or local).  Required. |
| `--es‑nodes`               | _None_  | Comma‑separated list of Elasticsearch hostnames.  Required. |
| `--es‑port`                | `9200`  | Elasticsearch port. |
| `--es‑index`               | _None_  | Name of the Elasticsearch index into which the logs will be written.  Required. |
| `--path‑column`            | `resource_path` | Name of the field in the audit log that contains the HDFS path.  If not present in the data, the job attempts to extract `path` from a nested `resource` object. |
| `--aws‑access‑key‑id`      | `$AWS_ACCESS_KEY_ID` | AWS access key for reading from S3.  Defaults to the environment variable of the same name. |
| `--aws‑secret‑access‑key`  | `$AWS_SECRET_ACCESS_KEY` | AWS secret key for reading from S3.  Defaults to the environment variable of the same name. |
| `--es‑username`            | _None_  | Username for basic authentication with Elasticsearch (optional). |
| `--es‑password`            | _None_  | Password for basic authentication with Elasticsearch (optional). |

## Running the Job

The job is designed to be launched with `spark‑submit`.  At a minimum you must provide the S3 audit path, the mapping CSV path, the Elasticsearch hosts, and the target index.  You also need to include the `elasticsearch‑hadoop` connector via the `--packages` option.  Here is a typical invocation:

```bash
spark‑submit \
  --packages org.elasticsearch:elasticsearch‑spark‑30_2.12:8.13.2,org.apache.hadoop:hadoop‑aws:3.3.4 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.access.key=<YOUR_AWS_ACCESS_KEY_ID> \
  --conf spark.hadoop.fs.s3a.secret.key=<YOUR_AWS_SECRET_ACCESS_KEY> \
  ranger_ingest.py \
    --s3‑audit‑path s3://my‑bucket/ranger/audit/2025/07/ \
    --mapping‑path s3://my‑bucket/config/table_mapping.csv \
    --es‑nodes es01.my‑domain.com,es02.my‑domain.com \
    --es‑index ranger‑audit \
    --es‑username elastic \
    --es‑password <ELASTIC_PASSWORD>
```

### Notes

* **AWS Credentials:** The Spark job uses the Hadoop S3A connector.  If you do not specify `--aws‑access‑key‑id` and `--aws‑secret‑access‑key`, the connector attempts to pick up credentials from the environment or the default AWS provider chain (instance profiles, etc.).
* **Elasticsearch Version:** The example uses the `8.13.2` connector for Spark 3.x.  Adjust the connector version to match your Elasticsearch deployment.  See the [Elastic documentation](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/install.html) for details.
* **Mapping File Size:** The mapping file is collected to the driver and broadcast to executors.  It should comfortably fit in memory.  For very large catalogs, consider indexing the mapping into a database or Hive table and performing a join instead of broadcasting.

## Project Structure

```
ranger_audit_ingest/
├── README.md        # This file
├── requirements.txt # Python dependencies
└── ranger_ingest.py # Main Spark job
```

## License

This project is provided as an example and carries no warranty.  You are free to adapt it to your needs.