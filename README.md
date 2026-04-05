# 实时热门统计系统

基于 **Spark Structured Streaming + Kafka** 的实时数据分析系统，实现电商场景下的热门商品和热搜词实时统计。

## 技术架构

```
Kafka Producer ──→ Kafka (KRaft) ──→ Spark Structured Streaming ──→ JSON Output ──→ Streamlit Dashboard
     │                                        │
     └── sample_batch.json ──→ Spark Batch ───┘ (性能对比)
```

**技术栈**: Kafka 3.6 (KRaft) | Spark 3.5 (PySpark) | Docker Compose | Streamlit

## 快速开始

### 1. 启动环境

```bash
# 确保 Docker Desktop 已分配至少 6G 内存
docker compose up -d

# 检查服务状态
make status
```

### 2. 启动数据生产者

```bash
# 安装 Python 依赖（本地运行 Producer）
pip install confluent-kafka

# 启动 Producer（100 条/秒）
make producer

# 高速模式（1000 条/秒，用于性能测试）
make producer-fast
```

### 3. 提交 Spark 流处理任务

```bash
# 热门商品统计
make submit-hot-products

# 热搜词统计
make submit-hot-keywords
```

### 4. 查看结果

```bash
# Spark UI: http://localhost:8080

# 启动 Streamlit 仪表盘
pip install streamlit pandas matplotlib
make dashboard
# 访问 http://localhost:8501
```

### 5. 批处理对比

```bash
# 先用 Producer 生成足够数据（运行几分钟后 Ctrl+C 停止）
# 然后执行批处理
make submit-batch
```

### 6. 性能测试

```bash
make benchmark
```

### 7. 容错演示

```bash
# 停止 Worker（模拟故障）
make fault-stop

# 重启 Worker（观察自动恢复）
make fault-start
```

## 核心功能

| 功能 | 说明 |
|---|---|
| 热门商品 Top-10 | 滑动窗口 (5min/1min) + Watermark (30s) |
| 热搜词 Top-10 | 同上，从搜索事件统计 |
| 批处理对比 | 相同统计逻辑，对比延迟和吞吐量 |
| 性能测量 | 端到端延迟 (avg/P95/P99) + 吞吐量 |
| 容错恢复 | Checkpoint 机制，Worker 故障后自动恢复 |
| 实时仪表盘 | Streamlit 自动刷新可视化 |

## 项目结构

```
src/
├── producer/          # Kafka 数据生产者（Zipf 分布模拟热门效应）
├── streaming/         # Spark Streaming 核心（热门商品 + 热搜词）
├── batch/             # 批处理对比脚本
├── visualization/     # Streamlit 仪表盘
└── benchmark/         # 性能测量
```
