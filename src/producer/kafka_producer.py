"""
Kafka 数据生产者：向 user_browse 和 user_search 两个 topic 发送模拟数据。
同时将数据写入 JSON 文件供批处理使用。

用法:
    python kafka_producer.py --rate 100 --late-ratio 0.1 --duration 300
"""

import argparse
import json
import os
import sys
import time

from confluent_kafka import Producer
from data_generator import generate_browse_event, generate_search_event

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Kafka 配置
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "data-producer",
    "acks": "all",
}

BROWSE_TOPIC = "user_browse"
SEARCH_TOPIC = "user_search"


def delivery_report(err, msg):
    """Kafka 消息投递回调"""
    if err is not None:
        print(f"投递失败: {err}")


def run_producer(rate, late_ratio, duration, save_batch):
    """
    运行数据生产者。

    Args:
        rate: 每秒发送的消息总数（浏览:搜索 = 7:3）
        late_ratio: 迟到数据比例
        duration: 运行时长（秒），0 表示无限
        save_batch: 是否同时保存到文件供批处理
    """
    producer = Producer(KAFKA_CONFIG)

    browse_rate = int(rate * 0.7)
    search_rate = rate - browse_rate
    interval = 1.0 / rate if rate > 0 else 0.01

    batch_file = None
    if save_batch:
        batch_path = os.path.join(BASE_DIR, "data", "sample_batch.json")
        batch_file = open(batch_path, "w", encoding="utf-8")

    print(f"开始发送数据: 速率={rate}条/秒 (浏览:{browse_rate}, 搜索:{search_rate})")
    print(f"迟到数据比例: {late_ratio*100:.0f}%")
    if duration > 0:
        print(f"持续时间: {duration}秒")
    print("按 Ctrl+C 停止\n")

    start_time = time.time()
    count = 0
    browse_count = 0
    search_count = 0

    try:
        while True:
            if duration > 0 and (time.time() - start_time) >= duration:
                break

            # 按比例交替发送浏览和搜索事件
            if count % 10 < 7:
                event = generate_browse_event(late_ratio)
                topic = BROWSE_TOPIC
                browse_count += 1
            else:
                event = generate_search_event(late_ratio)
                topic = SEARCH_TOPIC
                search_count += 1

            msg = json.dumps(event, ensure_ascii=False)
            producer.produce(topic, value=msg.encode("utf-8"), callback=delivery_report)

            if batch_file:
                batch_file.write(json.dumps({"topic": topic, **event}, ensure_ascii=False) + "\n")

            count += 1

            # 每 100 条 flush 一次
            if count % 100 == 0:
                producer.flush()

            # 每秒打印进度
            if count % rate == 0:
                elapsed = time.time() - start_time
                print(f"[{elapsed:.0f}s] 已发送: {count} 条 (浏览:{browse_count}, 搜索:{search_count})")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\n正在停止...")

    finally:
        producer.flush()
        if batch_file:
            batch_file.close()

        elapsed = time.time() - start_time
        print(f"\n发送完毕: 共 {count} 条, 耗时 {elapsed:.1f}秒")
        print(f"  浏览事件: {browse_count} 条")
        print(f"  搜索事件: {search_count} 条")
        print(f"  实际速率: {count/elapsed:.1f} 条/秒")


def main():
    parser = argparse.ArgumentParser(description="Kafka 数据生产者")
    parser.add_argument("--rate", type=int, default=100, help="每秒发送消息数 (默认: 100)")
    parser.add_argument("--late-ratio", type=float, default=0.1, help="迟到数据比例 (默认: 0.1)")
    parser.add_argument("--duration", type=int, default=0, help="运行时长秒数，0=无限 (默认: 0)")
    parser.add_argument("--no-batch", action="store_true", help="不保存批处理文件")
    args = parser.parse_args()

    run_producer(
        rate=args.rate,
        late_ratio=args.late_ratio,
        duration=args.duration,
        save_batch=not args.no_batch,
    )


if __name__ == "__main__":
    main()
