"""
数据生成器：使用加权随机模拟用户浏览和搜索行为。
权重越高的商品/关键词越容易被选中，模拟真实的热门效应。
"""

import json
import random
import os
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载商品和关键词字典
with open(os.path.join(BASE_DIR, "data", "products.json"), "r", encoding="utf-8") as f:
    PRODUCTS = json.load(f)

with open(os.path.join(BASE_DIR, "data", "search_keywords.json"), "r", encoding="utf-8") as f:
    KEYWORDS = json.load(f)

# 预计算加权列表
_product_weights = [p["weight"] for p in PRODUCTS]
_keyword_weights = [k["weight"] for k in KEYWORDS]

PLATFORMS = ["mobile", "pc", "tablet"]
EVENT_TYPES = ["view", "click", "add_cart", "purchase"]
EVENT_WEIGHTS = [50, 30, 15, 5]  # view 最多，purchase 最少


def generate_user_id():
    """生成随机用户 ID"""
    return f"u_{random.randint(10000, 99999)}"


def generate_browse_event(late_ratio=0.0):
    """
    生成一条用户浏览事件。

    Args:
        late_ratio: 迟到数据比例 (0.0-1.0)，用于测试 watermark 机制
    """
    product = random.choices(PRODUCTS, weights=_product_weights, k=1)[0]

    # 时间戳：大部分是当前时间，部分是迟到数据
    now = datetime.now()
    if random.random() < late_ratio:
        # 迟到数据：延迟 10-60 秒
        delay = random.randint(10, 60)
        event_time = now - timedelta(seconds=delay)
    else:
        event_time = now

    return {
        "user_id": generate_user_id(),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
        "timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        "producer_timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
    }


def generate_search_event(late_ratio=0.0):
    """
    生成一条用户搜索事件。

    Args:
        late_ratio: 迟到数据比例 (0.0-1.0)
    """
    keyword = random.choices(KEYWORDS, weights=_keyword_weights, k=1)[0]

    now = datetime.now()
    if random.random() < late_ratio:
        delay = random.randint(10, 60)
        event_time = now - timedelta(seconds=delay)
    else:
        event_time = now

    return {
        "user_id": generate_user_id(),
        "keyword": keyword["keyword"],
        "platform": random.choice(PLATFORMS),
        "timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
        "producer_timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3],
    }
