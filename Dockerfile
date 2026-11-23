# ============================================================
# Apache Spark 4.2.0
# Java 21 + Python 3
# ============================================================
FROM spark:4.2.0-java21-python3

USER root

# ============================================================
# Packages système
# ============================================================
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        netcat-openbsd \
        rsync \
        procps \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ============================================================
# Python packages
# ============================================================
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        pandas \
        lz4==4.3.3 \
        pyspark==4.2.0 \
        "jupyterlab>=4.4,<5" \
        "notebook>=7,<8" \
        "jupyter_server>=2,<3" \
        "tornado>=6.4,<7" \
        findspark \
        pyarrow \
        scales \
        matplotlib

# ============================================================
# Variables Spark
# ============================================================
ENV SPARK_HOME=/opt/spark \
    SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_WORKER_WEBUI_PORT=8080 \
    SPARK_WORKER_PORT=7000 \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/tmp/spark-events" \
    PYTHONHASHSEED=1

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-src.zip"

# ============================================================
# Répertoires
# ============================================================
RUN mkdir -p \
        /opt/spark-apps \
        /opt/spark-data \
        /opt/spark/logs \
        /tmp/spark-events \
        /opt/spark/conf

WORKDIR /opt/spark-apps

# ============================================================
# Ports
# ============================================================
EXPOSE 8080
EXPOSE 7077
EXPOSE 7000
EXPOSE 4040
EXPOSE 8888
EXPOSE 18080

# ============================================================
# Image neutre :
# Docker Compose démarre master / worker / history
# ============================================================
COPY start-spark.sh /start-spark.sh
RUN chmod +x /start-spark.sh

ENTRYPOINT ["/start-spark.sh"]

CMD ["bash"]
