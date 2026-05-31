# ==============================================================================
# Vox-SN — Image Spark avec dépendances Python pour NLP / ML
# ==============================================================================
# Build : docker compose build spark-master
# ==============================================================================

FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends python3-pip \
    && pip3 install --no-cache-dir \
        numpy==1.24.4 \
        pandas==2.0.3 \
        kafka-python==2.0.2 \
        python-dotenv==1.0.0 \
    && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
