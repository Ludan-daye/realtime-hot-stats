"""
Streamlit 实时仪表盘：展示热门商品和热搜词统计结果。

用法:
    streamlit run src/visualization/dashboard.py --server.port 8501
"""

import json
import os
import glob
import time

import streamlit as st
import pandas as pd

# 项目根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STREAM_OUTPUT = os.path.join(BASE_DIR, "output", "stream_results")
BATCH_OUTPUT = os.path.join(BASE_DIR, "output", "batch_results")


def load_json_output(path):
    """读取 Spark 输出的 JSON 文件（可能有多个 part 文件）。"""
    if not os.path.exists(path):
        return pd.DataFrame()

    json_files = glob.glob(os.path.join(path, "part-*.json"))
    if not json_files:
        return pd.DataFrame()

    records = []
    for f in json_files:
        with open(f, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if line:
                    records.append(json.loads(line))

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def render_hot_products():
    """渲染热门商品 Top 10。"""
    st.header("🔥 实时热门商品 Top 10")

    df = load_json_output(os.path.join(STREAM_OUTPUT, "hot_products"))

    if df.empty:
        st.info("等待流处理输出数据...")
        return

    # 按浏览量排序
    df = df.sort_values("view_count", ascending=False).head(10)

    # 柱状图
    chart_data = df.set_index("product_name")["view_count"]
    st.bar_chart(chart_data)

    # 详细表格
    st.dataframe(
        df[["product_name", "category", "view_count"]].reset_index(drop=True),
        use_container_width=True,
    )


def render_hot_keywords():
    """渲染热搜词 Top 10。"""
    st.header("🔍 实时热搜词 Top 10")

    df = load_json_output(os.path.join(STREAM_OUTPUT, "hot_keywords"))

    if df.empty:
        st.info("等待流处理输出数据...")
        return

    df = df.sort_values("search_count", ascending=False).head(10)

    # 柱状图
    chart_data = df.set_index("keyword")["search_count"]
    st.bar_chart(chart_data)

    # 详细表格
    st.dataframe(
        df[["keyword", "search_count"]].reset_index(drop=True),
        use_container_width=True,
    )


def render_batch_comparison():
    """渲染流处理 vs 批处理性能对比。"""
    st.header("⚡ 流处理 vs 批处理对比")

    # 加载批处理性能报告
    perf_path = os.path.join(BATCH_OUTPUT, "perf_report.json")
    if not os.path.exists(perf_path):
        st.info("请先运行批处理: make submit-batch")
        return

    with open(perf_path, "r") as f:
        batch_perf = json.load(f)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("批处理")
        st.metric("总记录数", f"{batch_perf['total_records']:,}")
        st.metric("总耗时", f"{batch_perf['total_time_sec']:.2f}s")
        st.metric("吞吐量", f"{batch_perf['throughput_per_sec']:,.0f} 条/秒")

    with col2:
        st.subheader("流处理")
        st.metric("处理模式", "微批次 (10s)")
        st.metric("延迟", "~10-15 秒")
        st.metric("特点", "实时、持续处理")

    # 批处理各阶段耗时
    st.subheader("批处理各阶段耗时")
    stages = {
        "数据加载": batch_perf["load_time_sec"],
        "热门商品统计": batch_perf["hot_products_time_sec"],
        "窗口商品统计": batch_perf["windowed_products_time_sec"],
        "热搜词统计": batch_perf["hot_keywords_time_sec"],
        "分类统计": batch_perf["category_stats_time_sec"],
    }
    st.bar_chart(pd.Series(stages))

    # 对比表格
    st.subheader("对比总结")
    comparison = pd.DataFrame({
        "指标": ["首条结果延迟", "端到端延迟", "吞吐量", "适用场景", "资源占用"],
        "流处理": ["秒级（窗口触发后）", "~10-15秒（微批次）", "受微批次限制", "实时监控、告警", "持续占用"],
        "批处理": [
            f"{batch_perf['total_time_sec']:.1f}秒（全量处理后）",
            f"{batch_perf['total_time_sec']:.1f}秒",
            f"{batch_perf['throughput_per_sec']:,.0f} 条/秒",
            "离线报表、历史分析",
            "按需启动",
        ],
    })
    st.table(comparison)


def render_streaming_perf():
    """渲染流处理性能指标。"""
    st.header("📊 流处理性能指标")

    perf_path = os.path.join(STREAM_OUTPUT, "perf_metrics.json")
    if not os.path.exists(perf_path):
        st.info("请先运行性能测试: make benchmark")
        return

    with open(perf_path, "r") as f:
        metrics = [json.loads(line) for line in f if line.strip()]

    if not metrics:
        st.info("暂无性能数据")
        return

    df = pd.DataFrame(metrics)

    if "avg_latency_ms" in df.columns:
        st.subheader("端到端延迟")
        st.line_chart(df[["avg_latency_ms", "p95_latency_ms", "p99_latency_ms"]])

    if "throughput" in df.columns:
        st.subheader("吞吐量 (条/秒)")
        st.line_chart(df["throughput"])


def main():
    st.set_page_config(
        page_title="实时热门统计仪表盘",
        page_icon="📊",
        layout="wide",
    )

    st.title("📊 实时热门统计仪表盘")
    st.caption("基于 Spark Structured Streaming + Kafka")

    # 自动刷新设置
    auto_refresh = st.sidebar.checkbox("自动刷新", value=True)
    refresh_interval = st.sidebar.slider("刷新间隔（秒）", 5, 60, 10)

    # 页面选择
    page = st.sidebar.radio(
        "页面",
        ["热门商品", "热搜词", "流批对比", "性能指标"],
    )

    if page == "热门商品":
        render_hot_products()
    elif page == "热搜词":
        render_hot_keywords()
    elif page == "流批对比":
        render_batch_comparison()
    elif page == "性能指标":
        render_streaming_perf()

    # 自动刷新
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
