"""
实时热搜词统计。

使用 Spark Structured Streaming + 滑动窗口 + Watermark。
从 Kafka user_search topic 读取搜索事件，
按 5 分钟窗口统计关键词频次，输出 Top 10 及平台分布。
"""

from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, desc, current_timestamp, lit
)

from common import (
    create_spark_session, read_kafka_stream,
    SEARCH_SCHEMA, SEARCH_TOPIC, STREAM_OUTPUT, CHECKPOINT_BASE
)

OUTPUT_PATH = f"{STREAM_OUTPUT}/hot_keywords"
PLATFORM_OUTPUT_PATH = f"{STREAM_OUTPUT}/keyword_platforms"
CHECKPOINT_PATH = f"{CHECKPOINT_BASE}/hot_keywords"


def main():
    spark = create_spark_session("HotKeywordsStreaming")

    # 1. 从 Kafka 读取原始数据
    raw_df = read_kafka_stream(spark, SEARCH_TOPIC)

    # 2. 解析 JSON
    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), SEARCH_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .withColumn("producer_ts", to_timestamp(col("producer_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    )

    # 3. Watermark + 滑动窗口：关键词频次统计
    keyword_counts = (
        parsed_df
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("keyword"),
        )
        .agg(count("*").alias("search_count"))
    )

    # 4. foreachBatch 处理
    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        latest_window = batch_df.select("window").orderBy(desc("window")).first()
        if latest_window is None:
            return

        # Top 10 热搜词
        top10 = (
            batch_df
            .filter(col("window") == latest_window["window"])
            .orderBy(desc("search_count"))
            .limit(10)
            .withColumn("batch_id", lit(batch_id))
            .withColumn("process_time", current_timestamp())
        )

        (
            top10
            .select("keyword", "search_count", "window", "batch_id", "process_time")
            .coalesce(1)
            .write.mode("overwrite")
            .json(OUTPUT_PATH)
        )

        print(f"\n===== Batch {batch_id} | 热搜词 Top 10 =====")
        top10.select("keyword", "search_count").show(10, truncate=False)

    # 5. 启动流处理
    query = (
        keyword_counts.writeStream
        .outputMode("update")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("热搜词流处理已启动，等待数据...")
    print(f"输出路径: {OUTPUT_PATH}")
    print(f"Checkpoint: {CHECKPOINT_PATH}")
    print("按 Ctrl+C 停止\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
