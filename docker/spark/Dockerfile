FROM bitnami/spark:3.5

USER root

# 安装 Python 依赖
RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    pandas==2.1.0

USER 1001
