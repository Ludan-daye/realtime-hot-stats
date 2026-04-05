"""
Spark Streaming 公共模块：SparkSession 创建、Schema 定义、Kafka 读取封装。
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

# ===== 项目根目录 =====
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ===== Kafka 配置 =====
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
BROWSE_TOPIC = "user_browse"
SEARCH_TOPIC = "user_search"

# ===== 输出路径 =====
OUTPUT_BASE = os.path.join(BASE_DIR, "output")
STREAM_OUTPUT = os.path.join(OUTPUT_BASE, "stream_results")
BATCH_OUTPUT = os.path.join(OUTPUT_BASE, "batch_results")
CHECKPOINT_BASE = os.path.join(OUTPUT_BASE, "checkpoints")

# ===== Schema 定义 =====
BROWSE_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("producer_timestamp", StringType(), True),
])

SEARCH_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("keyword", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("producer_timestamp", StringType(), True),
])


def create_spark_session(app_name):
    """创建 SparkSession，统一配置。"""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark, topic):
    """从 Kafka topic 读取流数据。"""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
