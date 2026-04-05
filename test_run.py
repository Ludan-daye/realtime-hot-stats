"""
一键测试：启动 Producer + Spark Streaming，验证整个流程。
"""
import os
import sys
import json
import time
import threading

# 设置 Spark Kafka 包
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 pyspark-shell"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, "src", "producer"))
sys.path.insert(0, os.path.join(BASE_DIR, "src", "streaming"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, desc, current_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType

from data_generator import generate_browse_event, generate_search_event
from confluent_kafka import Producer as KafkaProducer


def run_producer(rate=50, duration=50):
    """后台线程运行 Producer"""
    print(f"[Producer] 启动: {rate} 条/秒, 持续 {duration} 秒")

    config = {"bootstrap.servers": "localhost:9092", "client.id": "test-producer"}
    producer = KafkaProducer(config)

    interval = 1.0 / rate
    start = time.time()
    sent = 0

    while time.time() - start < duration:
        event = generate_browse_event(late_ratio=0.1)
        msg = json.dumps(event, ensure_ascii=False)
        producer.produce("user_browse", value=msg.encode("utf-8"))
        sent += 1
        if sent % 100 == 0:
            producer.flush()
            elapsed = time.time() - start
            print(f"[Producer] {elapsed:.0f}s - 已发送 {sent} 条")
        time.sleep(interval)

    producer.flush()
    print(f"[Producer] 完成: 共 {sent} 条")


def run_streaming():
    """运行 Spark Streaming"""
    print("[Spark] 创建 SparkSession...")

    spark = (
        SparkSession.builder
        .appName("TestHotProducts")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    BROWSE_SCHEMA = StructType([
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("producer_timestamp", StringType(), True),
    ])

    output_path = os.path.join(BASE_DIR, "output", "stream_results", "hot_products")
    checkpoint_path = os.path.join(BASE_DIR, "output", "checkpoints", "test")

    print("[Spark] 连接 Kafka...")
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "user_browse")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), BROWSE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    )

    windowed_counts = (
        parsed_df
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("product_id"),
            col("product_name"),
            col("category"),
        )
        .agg(count("*").alias("view_count"))
    )

    batch_counter = {"n": 0}

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        batch_counter["n"] += 1

        top10 = (
            batch_df
            .orderBy(desc("view_count"))
            .limit(10)
        )

        # 写入 JSON
        (
            top10
            .select("product_name", "category", "view_count")
            .coalesce(1)
            .write.mode("overwrite")
            .json(output_path)
        )

        print(f"\n{'='*60}")
        print(f"  批次 {batch_counter['n']} | 热门商品 Top 10")
        print(f"{'='*60}")
        top10.select("product_name", "category", "view_count").show(10, truncate=False)

    print("[Spark] 启动流处理...")
    query = (
        windowed_counts.writeStream
        .outputMode("update")
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[Spark] 流处理已启动! 等待数据...\n")
    query.awaitTermination(timeout=70)  # 最多等 70 秒
    query.stop()
    spark.stop()
    print("\n[Spark] 已停止")


def main():
    # 清理旧 checkpoint
    import shutil
    checkpoint_dir = os.path.join(BASE_DIR, "output", "checkpoints", "test")
    if os.path.exists(checkpoint_dir):
        shutil.rmtree(checkpoint_dir)

    # 先启动 Spark（后台线程），再启动 Producer
    spark_thread = threading.Thread(target=run_streaming, daemon=True)
    spark_thread.start()

    # 等 Spark 初始化
    time.sleep(15)

    # 启动 Producer
    producer_thread = threading.Thread(target=run_producer, args=(50, 50), daemon=True)
    producer_thread.start()

    # 等待两个线程完成
    producer_thread.join(timeout=60)
    spark_thread.join(timeout=90)

    # 检查输出
    output_path = os.path.join(BASE_DIR, "output", "stream_results", "hot_products")
    if os.path.exists(output_path):
        print(f"\n{'='*60}")
        print("  输出文件:")
        print(f"{'='*60}")
        for f in os.listdir(output_path):
            fpath = os.path.join(output_path, f)
            if f.endswith(".json"):
                with open(fpath, "r", encoding="utf-8") as fp:
                    for line in fp:
                        print(line.strip())
    else:
        print("\n没有找到输出文件")


if __name__ == "__main__":
    main()
