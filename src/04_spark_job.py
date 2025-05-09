"""
Spark streaming job to process IoT data from Kafka and detect anomalies.
Processes temperature data and identifies anomalies based on threshold.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col, udf, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, BooleanType
from dotenv import load_dotenv

load_dotenv()

# Configuration
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot_data")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "IoTAnomalyDetection")
SPARK_PACKAGES = os.getenv("SPARK_PACKAGES", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL", "WARN")

MINIO_URL = os.getenv("MINIO_URL", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "iot-processed-data")
MINIO_PROCESSED_OUTPUT_PATH = os.getenv("MINIO_PROCESSED_OUTPUT_PATH", "processed_data")
MINIO_CHECKPOINT_PATH = os.getenv("MINIO_CHECKPOINT_PATH", "checkpoints/spark_iot")

TEMPERATURE_ANOMALY_THRESHOLD = float(os.getenv("TEMPERATURE_ANOMALY_THRESHOLD", 35.0))

# Schema for incoming IoT data
IOT_DATA_SCHEMA = StructType([
    StructField("deviceId", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("status", StringType(), True)
])

@udf(returnType=StringType())
def determine_anomaly_severity(temperature: float, current_status: str) -> str:
    """Determines the severity of an anomaly based on temperature and current status.
    
    Args:
        temperature: The temperature reading
        current_status: Current status from the device
        
    Returns:
        Severity level as string: "normal", "warning", or "critical"
    """
    if temperature is None:
        return "unknown"
    elif temperature > TEMPERATURE_ANOMALY_THRESHOLD + 5:
        return "critical"
    elif temperature > TEMPERATURE_ANOMALY_THRESHOLD:
        return "warning"
    else:
        return "normal"

def create_spark_session(app_name: str, packages: str, minio_conf: dict) -> SparkSession:
    """Creates and configures a Spark session with MinIO integration."""
    builder = SparkSession.builder.appName(app_name).config("spark.jars.packages", packages)
    
    # Configure MinIO connection
    minio_endpoint = f"http://{minio_conf['endpoint']}" if not minio_conf['endpoint'].startswith(('http://', 'https://')) else minio_conf['endpoint']
    
    return builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
                 .config("spark.hadoop.fs.s3a.access.key", minio_conf["access_key"]) \
                 .config("spark.hadoop.fs.s3a.secret.key", minio_conf["secret_key"]) \
                 .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                 .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                 .getOrCreate()

def read_kafka_stream(spark: SparkSession, broker_url: str, topic: str) -> DataFrame:
    """Creates a streaming DataFrame from Kafka source."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker_url) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

def process_stream(kafka_df: DataFrame, schema: StructType) -> DataFrame:
    """Processes the streaming data and adds anomaly detection columns."""
    # Parse JSON data
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Add processing timestamp and anomaly detection
    processed_df = parsed_df \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("anomaly_severity", determine_anomaly_severity(col("temperature"), col("status"))) \
        .withColumn("is_anomaly", col("anomaly_severity").isin(["warning", "critical"]))
    
    return processed_df

def write_to_minio(df: DataFrame, bucket_name: str, output_path: str, checkpoint_path: str):
    """Writes the processed DataFrame to MinIO in Parquet format."""
    query = df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"s3a://{bucket_name}/{output_path}") \
        .option("checkpointLocation", f"s3a://{bucket_name}/{checkpoint_path}") \
        .start()
    
    print(f"Started writing to MinIO - Path: s3a://{bucket_name}/{output_path}")
    return query

def main():
    """Main function to set up and run the Spark streaming job."""
    # MinIO configuration
    minio_conf = {
        "endpoint": MINIO_URL,
        "access_key": MINIO_ACCESS_KEY,
        "secret_key": MINIO_SECRET_KEY
    }
    
    # Create Spark session
    spark = create_spark_session(SPARK_APP_NAME, SPARK_PACKAGES, minio_conf)
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    
    try:
        # Set up streaming pipeline
        kafka_stream_df = read_kafka_stream(spark, KAFKA_BROKER_URL, KAFKA_TOPIC)
        processed_df = process_stream(kafka_stream_df, IOT_DATA_SCHEMA)
        
        # Start writing to MinIO
        query = write_to_minio(
            processed_df,
            MINIO_BUCKET_NAME,
            MINIO_PROCESSED_OUTPUT_PATH,
            MINIO_CHECKPOINT_PATH
        )
        
        query.awaitTermination()
    except Exception as e:
        print(f"Error in Spark job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()