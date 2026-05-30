"""
streaming_sentiment.py — Pipeline NLP Spark Streaming Vox-SN
=============================================================

Pipeline temps réel qui :
    1. Consomme les posts citoyens depuis Kafka `social_raw`
    2. Applique la Privacy Layer (SHA-256 + drop PII)
    3. Nettoie le texte (regex + stopwords FR/Wolof)
    4. Score le sentiment (lexique Vox-SN)
    5. Catégorise la plainte (TARIF/TECHNIQUE/FRAUDE/SERVICE_CLIENT)
    6. Détermine le statut d'alerte (CRISE/NEGATIF_FORT/NORMAL)
    7. Écrit les posts analysés vers Kafka `social_analyzed`
    8. Calcule des agrégats fenêtre 1h vers `social_sentiment_agg`

⚠️ PRIVACY CRITIQUE
    Ce pipeline traite des données financières sensibles (Mobile Money).
    Le `drop('user_id', 'phone_number')` après hash SHA-256 est NON
    NÉGOCIABLE. Aucun PII brut ne doit apparaître dans HBase ni Hive.

Lancement :
    spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        streaming_sentiment.py

Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
Auteur    : Vox-SN Team - UADB M2 BD&IA 2025-2026
"""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sha2, concat, lit, from_json, to_json, struct,
    lower, regexp_replace, trim,
    when, current_timestamp, udf, window, length,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
)

# Import des modules Vox-SN (doivent être dans le PYTHONPATH du driver)
# En production : spark-submit --py-files lexique_sn.py,schema.py streaming_sentiment.py
try:
    from lexique_sn import NEGATIF, POSITIF, CATEGORIES, ALL_STOPWORDS
except ImportError:
    # Fallback : chargement direct du fichier voisin
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from lexique_sn import NEGATIF, POSITIF, CATEGORIES, ALL_STOPWORDS


# =============================================================================
# Configuration
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("VoxStreaming")

SALT = os.environ.get("CITIZEN_SECRET_SALT", "UADB_VOX_2025_default")
BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC_IN = os.environ.get("KAFKA_TOPIC_RAW", "social_raw")
TOPIC_OUT = os.environ.get("KAFKA_TOPIC_ANALYZED", "social_analyzed")
TOPIC_AGG = os.environ.get("KAFKA_TOPIC_AGG", "social_sentiment_agg")
CHECKPOINT_DIR = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/vox_sn_ckpt")

CRISIS_THRESHOLD = float(os.environ.get("SENTIMENT_CRISIS_THRESHOLD", "-0.5"))
WARNING_THRESHOLD = float(os.environ.get("SENTIMENT_WARNING_THRESHOLD", "-0.2"))


# =============================================================================
# SparkSession
# =============================================================================
def build_spark() -> SparkSession:
    """Construit la SparkSession optimisée pour le streaming Vox-SN."""
    spark = (
        SparkSession.builder
        .appName("Vox_SN_NLP_Streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession démarrée — version : %s", spark.version)
    return spark


# =============================================================================
# Schéma JSON des posts entrants
# =============================================================================
POST_SCHEMA = StructType([
    StructField("post_id", StringType(), True),
    StructField("user_id", StringType(), True),        # SUPPRIMÉ après hash
    StructField("phone_number", StringType(), True),   # SUPPRIMÉ après hash
    StructField("service_cible", StringType(), True),
    StructField("texte_du_post", StringType(), True),
    StructField("langue", StringType(), True),
    StructField("canal", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("region", StringType(), True),
])


# =============================================================================
# UDFs NLP — broadcast des lexiques
# =============================================================================
def _make_udfs(spark: SparkSession) -> dict:
    """Crée et enregistre les UDFs (closures sur broadcasts)."""

    bc_negatif = spark.sparkContext.broadcast(NEGATIF)
    bc_positif = spark.sparkContext.broadcast(POSITIF)
    bc_categories = spark.sparkContext.broadcast(CATEGORIES)
    bc_stopwords = spark.sparkContext.broadcast(ALL_STOPWORDS)

    @udf(returnType=FloatType())
    def score_sentiment(texte: str) -> float:
        """Score lexical du sentiment ∈ [-1, +1]."""
        if not texte:
            return 0.0
        texte_lower = texte.lower()
        score = 0.0
        matches = 0
        for terme, val in bc_negatif.value.items():
            if terme in texte_lower:
                score += val
                matches += 1
        for terme, val in bc_positif.value.items():
            if terme in texte_lower:
                score += val
                matches += 1
        if matches == 0:
            return 0.0
        # Clamp [-1, +1]
        avg = score / matches
        return float(max(-1.0, min(1.0, avg)))

    @udf(returnType=StringType())
    def categoriser(texte: str) -> str:
        """Catégorise la plainte selon la taxonomie Vox-SN."""
        if not texte:
            return "INCONNU"
        texte_lower = texte.lower()
        scores = {
            cat: sum(1 for kw in mots if kw in texte_lower)
            for cat, mots in bc_categories.value.items()
        }
        best = max(scores, key=scores.get)
        return best if scores[best] > 0 else "AUTRE"

    @udf(returnType=StringType())
    def nettoyer_texte(texte: str) -> str:
        """Supprime les stopwords FR+Wolof et garde tokens de longueur > 2."""
        if not texte:
            return ""
        tokens = texte.lower().split()
        tokens = [
            t for t in tokens
            if t not in bc_stopwords.value and len(t) > 2 and t.isalpha()
        ]
        return " ".join(tokens)

    return {
        "score_sentiment": score_sentiment,
        "categoriser": categoriser,
        "nettoyer_texte": nettoyer_texte,
    }


# =============================================================================
# Pipeline principal
# =============================================================================
def main() -> None:
    spark = build_spark()
    udfs = _make_udfs(spark)

    # -------------------------------------------------------------------------
    # 1. Lecture du flux Kafka social_raw
    # -------------------------------------------------------------------------
    logger.info("Lecture Kafka : %s → %s", BROKERS, TOPIC_IN)
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), POST_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_ts", current_timestamp())
        # Filtre minimal de qualité (au cas où Pandera amont défaille)
        .filter(col("texte_du_post").isNotNull())
        .filter(length(col("texte_du_post")) >= 5)
    )

    # -------------------------------------------------------------------------
    # 2. Privacy Layer — anonymisation SHA-256 + drop PII
    # -------------------------------------------------------------------------
    secure_df = (
        raw_df
        .withColumn(
            "citizen_id_secure",
            sha2(concat(col("user_id"), lit(SALT)), 256),
        )
        # ⚠️ NON NÉGOCIABLE : drop des deux champs PII bruts
        .drop("user_id", "phone_number")
    )

    # -------------------------------------------------------------------------
    # 3. Normalisation du texte (conserver accents FR)
    # -------------------------------------------------------------------------
    clean_df = secure_df.withColumn(
        "texte_norm",
        trim(regexp_replace(
            lower(col("texte_du_post")),
            r"[^a-zàáâäèéêëîïôùûüœçñ\s']",
            " ",
        )),
    )

    # -------------------------------------------------------------------------
    # 4. Application des UDFs NLP
    # -------------------------------------------------------------------------
    nlp_df = (
        clean_df
        .withColumn("texte_clean", udfs["nettoyer_texte"](col("texte_norm")))
        .withColumn("sentiment_score", udfs["score_sentiment"](col("texte_norm")))
        .withColumn("categorie", udfs["categoriser"](col("texte_norm")))
        .withColumn(
            "sentiment_label",
            when(col("sentiment_score") < CRISIS_THRESHOLD, lit("NEGATIF_FORT"))
            .when(col("sentiment_score") < 0.0, lit("NEGATIF"))
            .when(col("sentiment_score") > 0.3, lit("POSITIF"))
            .otherwise(lit("NEUTRE")),
        )
        .withColumn(
            "statut_alerte",
            when(
                (col("sentiment_score") < CRISIS_THRESHOLD)
                & (col("categorie").isin("FRAUDE", "TECHNIQUE")),
                lit("CRISE"),
            )
            .when(col("sentiment_score") < CRISIS_THRESHOLD, lit("NEGATIF_FORT"))
            .otherwise(lit("NORMAL")),
        )
    )

    # -------------------------------------------------------------------------
    # 5. Agrégation fenêtre glissante 1h / service
    # -------------------------------------------------------------------------
    sentiment_window = (
        nlp_df
        .withWatermark("event_ts", "10 minutes")
        .groupBy(
            window("event_ts", "1 hour", "15 minutes"),
            "service_cible",
        )
        .agg(
            {"sentiment_score": "avg", "post_id": "count"}
        )
        .withColumnRenamed("avg(sentiment_score)", "sentiment_moyen")
        .withColumnRenamed("count(post_id)", "nb_posts")
        .withColumn(
            "statut_operateur",
            when(col("sentiment_moyen") < CRISIS_THRESHOLD, lit("CRISE"))
            .when(col("sentiment_moyen") < WARNING_THRESHOLD, lit("ATTENTION"))
            .otherwise(lit("NORMAL")),
        )
    )

    # -------------------------------------------------------------------------
    # 6. Sortie 1 : posts analysés → Kafka social_analyzed
    # -------------------------------------------------------------------------
    query_posts = (
        nlp_df
        .select(to_json(struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("topic", TOPIC_OUT)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/posts")
        .outputMode("append")
        .start()
    )
    logger.info("Sortie 1 démarrée → %s", TOPIC_OUT)

    # -------------------------------------------------------------------------
    # 7. Sortie 2 : agrégats sentiment → Kafka social_sentiment_agg
    # -------------------------------------------------------------------------
    query_agg = (
        sentiment_window
        .select(to_json(struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("topic", TOPIC_AGG)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/agg")
        .outputMode("update")
        .start()
    )
    logger.info("Sortie 2 démarrée → %s", TOPIC_AGG)

    # -------------------------------------------------------------------------
    # 8. Sortie 3 : log console pour debug (5 lignes / batch)
    # -------------------------------------------------------------------------
    query_console = (
        nlp_df.select(
            "post_id", "service_cible", "langue", "sentiment_score",
            "categorie", "sentiment_label", "statut_alerte"
        )
        .writeStream
        .format("console")
        .option("numRows", 5)
        .option("truncate", "false")
        .outputMode("append")
        .start()
    )

    # -------------------------------------------------------------------------
    # Attente jusqu'à terminaison de l'un des queries
    # -------------------------------------------------------------------------
    logger.info("Pipeline NLP en cours d'exécution. Ctrl+C pour arrêter.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
