.PHONY: up down restart logs status submit-hot-products submit-hot-keywords submit-batch producer dashboard clean

# ===== Docker 环境 =====
up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

status:
	docker compose ps

# ===== 数据生产者 =====
producer:
	python3 src/producer/kafka_producer.py --rate 100

producer-fast:
	python3 src/producer/kafka_producer.py --rate 1000

# ===== Spark Streaming 任务 =====
submit-hot-products:
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/opt/spark-app/src/streaming/hot_products.py

submit-hot-keywords:
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/opt/spark-app/src/streaming/hot_keywords.py

# ===== 批处理 =====
submit-batch:
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark-app/src/batch/batch_analysis.py

# ===== 可视化 =====
dashboard:
	streamlit run src/visualization/dashboard.py --server.port 8501

# ===== 性能测试 =====
benchmark:
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		/opt/spark-app/src/benchmark/perf_test.py

# ===== 容错演示 =====
fault-stop:
	docker stop spark-worker

fault-start:
	docker start spark-worker

# ===== 清理 =====
clean:
	rm -rf output/stream_results/* output/batch_results/* output/checkpoints/*

# ===== Kafka 工具 =====
kafka-topics:
	docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-consume-browse:
	docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user_browse --from-beginning

kafka-consume-search:
	docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user_search --from-beginning
