# ==============================================================================
# Vox-SN — Dockerfile Airflow
# ==============================================================================
# Image Airflow étendue avec : pyhive, happybase, pandera, mlflow
# Permet aux DAGs d'exécuter des requêtes Hive et HBase sans subprocess.
#
# Build : docker build -f docker/airflow.Dockerfile -t vox-sn/airflow:latest .
# ==============================================================================

FROM apache/airflow:2.7.3-python3.9

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsasl2-dev \
    libsasl2-modules \
    gcc \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    pyhive==0.7.0 \
    thrift==0.16.0 \
    thrift_sasl==0.4.3 \
    happybase==1.2.0 \
    pandera==0.17.2 \
    mlflow==2.8.1 \
    apache-airflow-providers-apache-spark==4.4.0 \
    apache-airflow-providers-apache-hive==6.4.0
