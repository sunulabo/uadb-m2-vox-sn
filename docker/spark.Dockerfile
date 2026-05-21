# ==============================================================================
# Vox-SN — Dockerfile Spark avec dépendances NLP
# ==============================================================================
# Image Spark étendue avec : Python + Spark NLP + pyhive + happybase
# Utilisée pour les jobs spark-submit qui ont besoin d'écrire dans HBase/Hive.
#
# Build : docker build -f docker/spark.Dockerfile -t vox-sn/spark:latest .
# ==============================================================================

FROM bitnami/spark:3.3

USER root

# Installation des outils système
RUN install_packages python3-pip python3-dev gcc libsasl2-dev libsasl2-modules

# Installation des dépendances Python pour les jobs Spark
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir \
    kafka-python==2.0.2 \
    pandera==0.17.2 \
    happybase==1.2.0 \
    pyhive==0.7.0 \
    thrift==0.16.0 \
    nltk==3.8.1 \
    langdetect==1.0.9 \
    && python3 -c "import nltk; nltk.download('punkt', quiet=True); nltk.download('stopwords', quiet=True)"

# Téléchargement du JAR Kafka pour Spark Streaming
RUN mkdir -p /opt/bitnami/spark/jars/extra && cd /opt/bitnami/spark/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.0/spark-token-provider-kafka-0-10_2.12-3.3.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.0/kafka-clients-3.3.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

USER 1001

# Variables d'environnement
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
