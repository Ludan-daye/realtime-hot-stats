"""
批处理分析：用 Spark DataFrame API 对静态数据做同样的热门统计。
与流处理对比延迟和吞吐量。

数据来源：data/sample_batch.json（由 kafka_producer.py 生成）
"""

import time
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, window, count, desc, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType
)

# 路径配置
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_PATH = os.path.join(BASE_DIR, "data", "sample_batch.json")
OUTPUT_BASE = os.path.join(BASE_DIR, "output", "batch_results")


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BatchAnalysis")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def run_batch_analysis():
    spark = create_spark_session()

    print("=" * 60)
    print("批处理分析开始")
    print("=" * 60)

    # ===== 1. 读取数据 =====
    t_start = time.time()

    raw_df = spark.read.json(DATA_PATH)
    total_records = raw_df.count()
    t_load = time.time()
    print(f"\n数据加载: {total_records} 条, 耗时 {t_load - t_start:.2f}s")

    # 分离浏览和搜索数据
    browse_df = (
        raw_df
        .filter(col("topic") == "user_browse")
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    )
    search_df = (
        raw_df
        .filter(col("topic") == "user_search")
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    )

    browse_count = browse_df.count()
    search_count = search_df.count()
    print(f"  浏览事件: {browse_count} 条")
    print(f"  搜索事件: {search_count} 条")

    # ===== 2. 热门商品统计 =====
    print("\n--- 热门商品 Top 10（全量统计） ---")
    t1 = time.time()

    hot_products = (
        browse_df
        .groupBy("product_id", "product_name", "category")
        .agg(count("*").alias("view_count"))
        .orderBy(desc("view_count"))
        .limit(10)
    )
    hot_products.show(10, truncate=False)

    # 写入结果
    hot_products.coalesce(1).write.mode("overwrite").json(f"{OUTPUT_BASE}/hot_products")
    t2 = time.time()
    print(f"热门商品统计耗时: {t2 - t1:.2f}s")

    # ===== 3. 热门商品（窗口统计，与流处理对比） =====
    print("\n--- 热门商品（5分钟窗口统计） ---")
    t3 = time.time()

    windowed_products = (
        browse_df
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("product_id"),
            col("product_name"),
        )
        .agg(count("*").alias("view_count"))
        .orderBy(desc("view_count"))
        .limit(20)
    )
    windowed_products.show(20, truncate=False)

    windowed_products.coalesce(1).write.mode("overwrite").json(f"{OUTPUT_BASE}/windowed_products")
    t4 = time.time()
    print(f"窗口商品统计耗时: {t4 - t3:.2f}s")

    # ===== 4. 热搜词统计 =====
    print("\n--- 热搜词 Top 10 ---")
    t5 = time.time()

    hot_keywords = (
        search_df
        .groupBy("keyword")
        .agg(count("*").alias("search_count"))
        .orderBy(desc("search_count"))
        .limit(10)
    )
    hot_keywords.show(10, truncate=False)

    hot_keywords.coalesce(1).write.mode("overwrite").json(f"{OUTPUT_BASE}/hot_keywords")
    t6 = time.time()
    print(f"热搜词统计耗时: {t6 - t5:.2f}s")

    # ===== 5. 分类统计 =====
    print("\n--- 商品分类浏览量 ---")
    t7 = time.time()

    category_stats = (
        browse_df
        .groupBy("category")
        .agg(count("*").alias("total_views"))
        .orderBy(desc("total_views"))
    )
    category_stats.show(truncate=False)

    category_stats.coalesce(1).write.mode("overwrite").json(f"{OUTPUT_BASE}/category_stats")
    t8 = time.time()
    print(f"分类统计耗时: {t8 - t7:.2f}s")

    # ===== 6. 汇总性能报告 =====
    t_total = time.time()
    print("\n" + "=" * 60)
    print("批处理性能报告")
    print("=" * 60)
    print(f"总记录数:        {total_records}")
    print(f"数据加载时间:    {t_load - t_start:.2f}s")
    print(f"热门商品统计:    {t2 - t1:.2f}s")
    print(f"窗口商品统计:    {t4 - t3:.2f}s")
    print(f"热搜词统计:      {t6 - t5:.2f}s")
    print(f"分类统计:        {t8 - t7:.2f}s")
    print(f"总耗时:          {t_total - t_start:.2f}s")
    print(f"吞吐量:          {total_records / (t_total - t_start):.0f} 条/秒")

    # 保存性能数据为 JSON
    perf_report = {
        "total_records": total_records,
        "load_time_sec": round(t_load - t_start, 2),
        "hot_products_time_sec": round(t2 - t1, 2),
        "windowed_products_time_sec": round(t4 - t3, 2),
        "hot_keywords_time_sec": round(t6 - t5, 2),
        "category_stats_time_sec": round(t8 - t7, 2),
        "total_time_sec": round(t_total - t_start, 2),
        "throughput_per_sec": round(total_records / (t_total - t_start)),
    }

    perf_path = f"{OUTPUT_BASE}/perf_report.json"
    with open(perf_path, "w") as f:
        json.dump(perf_report, f, indent=2)
    print(f"\n性能报告已保存: {perf_path}")

    spark.stop()


if __name__ == "__main__":
    run_batch_analysis()
