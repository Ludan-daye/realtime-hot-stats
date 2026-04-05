"""
实时热门商品 Top-N 统计。

使用 Spark Structured Streaming + 滑动窗口 + Watermark。
从 Kafka user_browse topic 读取用户浏览事件，
按 5 分钟窗口（1 分钟滑动）统计各商品浏览次数，输出 Top 10。
"""

from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, desc, current_timestamp, lit
)

from common import (
    create_spark_session, read_kafka_stream,
    BROWSE_SCHEMA, BROWSE_TOPIC, STREAM_OUTPUT, CHECKPOINT_BASE
)

OUTPUT_PATH = f"{STREAM_OUTPUT}/hot_products"
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/hot_products"


def main():
    spark = create_spark_session("HotProductsStreaming")

    # 1. 从 Kafka 读取原始数据
    raw_df = read_kafka_stream(spark, BROWSE_TOPIC)

    # 2. 解析 JSON，提取字段
    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), BROWSE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .withColumn("producer_ts", to_timestamp(col("producer_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    )

    # 3. Watermark + 滑动窗口聚合
    #    - watermark: 30 秒，允许迟到 30 秒内的数据参与计算
    #    - 窗口: 5 分钟，每 1 分钟滑动一次
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

    # 4. foreachBatch: 每个微批次取 Top 10 并写出
    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # 取最新窗口的数据
        latest_window = batch_df.select("window").orderBy(desc("window")).first()
        if latest_window is None:
            return

        top10 = (
            batch_df
            .filter(col("window") == latest_window["window"])
            .orderBy(desc("view_count"))
            .limit(10)
            .withColumn("batch_id", lit(batch_id))
            .withColumn("process_time", current_timestamp())
        )

        # 写入 JSON（overwrite 保持只有最新结果）
        (
            top10
            .select(
                "product_id", "product_name", "category",
                "view_count", "window", "batch_id", "process_time"
            )
            .coalesce(1)
            .write.mode("overwrite")
            .json(OUTPUT_PATH)
        )

        # 打印到控制台方便调试
        print(f"\n===== Batch {batch_id} | 热门商品 Top 10 =====")
        top10.select("product_name", "category", "view_count").show(10, truncate=False)

    # 5. 启动流处理
    query = (
        windowed_counts.writeStream
        .outputMode("update")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("热门商品流处理已启动，等待数据...")
    print(f"输出路径: {OUTPUT_PATH}")
    print(f"Checkpoint: {CHECKPOINT_PATH}")
    print("按 Ctrl+C 停止\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
