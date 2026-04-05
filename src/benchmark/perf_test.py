"""
流处理性能测试：测量端到端延迟和吞吐量。

在 foreachBatch 中计算每条消息从 Producer 到处理完成的延迟，
并输出性能指标到文件供可视化使用。
"""

import json
import time
import os

from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    avg, expr, count, percentile_approx, lit
)
from pyspark.sql.types import DoubleType

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "streaming"))

from common import (
    create_spark_session, read_kafka_stream,
    BROWSE_SCHEMA, BROWSE_TOPIC, STREAM_OUTPUT
)

PERF_OUTPUT = f"{STREAM_OUTPUT}/perf_metrics.json"


def main():
    spark = create_spark_session("PerformanceBenchmark")

    # 从 Kafka 读取
    raw_df = read_kafka_stream(spark, BROWSE_TOPIC)

    parsed_df = (
        raw_df
        .select(from_json(col("value").cast("string"), BROWSE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .withColumn("producer_ts", to_timestamp(col("producer_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    )

    batch_count = {"n": 0}

    def measure_performance(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        batch_count["n"] += 1
        process_time = time.time()

        # 计算端到端延迟（当前时间 - producer_timestamp）
        latency_df = (
            batch_df
            .withColumn("process_ts", current_timestamp())
            .withColumn(
                "latency_ms",
                (col("process_ts").cast("double") - col("producer_ts").cast("double")) * 1000
            )
            .filter(col("latency_ms") > 0)
        )

        record_count = batch_df.count()

        if latency_df.count() > 0:
            stats = latency_df.agg(
                avg("latency_ms").alias("avg_latency"),
                percentile_approx("latency_ms", 0.95).alias("p95_latency"),
                percentile_approx("latency_ms", 0.99).alias("p99_latency"),
            ).first()

            metrics = {
                "batch_id": batch_id,
                "batch_num": batch_count["n"],
                "record_count": record_count,
                "avg_latency_ms": round(stats["avg_latency"], 2),
                "p95_latency_ms": round(stats["p95_latency"], 2),
                "p99_latency_ms": round(stats["p99_latency"], 2),
                "throughput": round(record_count / 10.0),  # 10s 触发间隔
                "timestamp": process_time,
            }

            # 追加写入性能指标文件
            with open(PERF_OUTPUT, "a") as f:
                f.write(json.dumps(metrics) + "\n")

            print(f"Batch {batch_id}: "
                  f"records={record_count}, "
                  f"avg_latency={metrics['avg_latency_ms']:.0f}ms, "
                  f"p95={metrics['p95_latency_ms']:.0f}ms, "
                  f"p99={metrics['p99_latency_ms']:.0f}ms, "
                  f"throughput={metrics['throughput']} rec/s")

    # 启动
    query = (
        parsed_df.writeStream
        .outputMode("append")
        .foreachBatch(measure_performance)
        .option("checkpointLocation", f"{STREAM_OUTPUT}/../checkpoints/perf_test")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("性能测试已启动...")
    print(f"指标输出: {PERF_OUTPUT}")
    print("按 Ctrl+C 停止\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
