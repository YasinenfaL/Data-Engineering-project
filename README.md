# Spark + Kafka + MinIO Akış Hattı

* This poc processes synthetic sensor data in real-time with Kafka ➜ Spark Structured Streaming ➜ MinIO chain and stores it in S3-compatible storage (MinIO). It runs on a single machine with Docker-Compose.

---

## Requirements

* Docker & Docker Compose
* Python ≥ 3.8

```bash
pip install -r requirements.txt
```

---

## Quick Start

```bash
$ docker compose up -d

$ ./scripts/01_create_topic.sh

$ python scripts/03_create_bucket.py

$ python scripts/02_producer.py

$ docker compose exec spark-master bash

$ spark-submit --packages \
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
  org.apache.hadoop:hadoop-aws:3.3.6,\
  com.amazonaws:aws-java-sdk-bundle:1.12.664 \
  /opt/bitnami/scripts/04_spark_job.py
```


