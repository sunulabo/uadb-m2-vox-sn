"""
train_classifier.py — Entraînement Spark ML Vox-SN
===================================================

Entraîne et persiste deux modèles Spark ML en parallèle :

    Modèle 1 : Logistic Regression
        Cible    : sentiment_label ∈ {POSITIF, NEGATIF, NEGATIF_FORT, NEUTRE}
        Features : TF-IDF (HashingTF + IDF, 5000 features)

    Modèle 2 : Random Forest Classifier
        Cible    : categorie ∈ {TARIF, TECHNIQUE, FRAUDE, SERVICE_CLIENT, ...}
        Features : TF-IDF (mêmes features)

Métriques : F1 multi-classe. Cible projet : F1 > 0.70 pour validation.

Sauvegarde :
    - Pipelines Spark ML       → output_path/sentiment, output_path/categorie
    - Tracking MLflow          → URI configurable
    - Métadonnées dans JSON    → output_path/training_metadata.json

Lancement :
    spark-submit train_classifier.py \\
        --output-path /opt/models/classifier_latest \\
        --window-days 30 \\
        --hive-database vox_sn

Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
Auteur    : Vox-SN Team - UADB M2 BD&IA 2025-2026
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer,
)
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# =============================================================================
# Configuration
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("VoxTrainer")


# =============================================================================
# Parsing CLI
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Entraînement classifieurs Vox-SN")
    parser.add_argument(
        "--output-path",
        default="/opt/models/classifier_latest",
        help="Chemin de sauvegarde des modèles.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="Fenêtre temporelle des données d'entraînement (jours).",
    )
    parser.add_argument(
        "--hive-database",
        default="vox_sn",
        help="Base Hive contenant la table posts_analyses.",
    )
    parser.add_argument(
        "--mlflow-uri",
        default=os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
        help="URI MLflow (vide = pas de tracking).",
    )
    parser.add_argument(
        "--num-features",
        type=int,
        default=5000,
        help="Nombre de features HashingTF.",
    )
    parser.add_argument(
        "--num-trees",
        type=int,
        default=100,
        help="Nombre d'arbres Random Forest.",
    )
    parser.add_argument(
        "--csv-path",
        default="/opt/data/samples/training_data.csv",
        help="Fallback CSV si Hive est vide ou inaccessible.",
    )
    return parser.parse_args()


# =============================================================================
# Construction des pipelines
# =============================================================================
def build_sentiment_pipeline(num_features: int) -> Pipeline:
    """Pipeline Logistic Regression pour la classification du sentiment."""
    label_idx = StringIndexer(
        inputCol="sentiment_label", outputCol="label_sent",
        handleInvalid="keep",
    )
    tokenizer = Tokenizer(inputCol="texte_clean", outputCol="tokens")
    remover = StopWordsRemover(inputCol="tokens", outputCol="tokens_clean")
    hashing_tf = HashingTF(
        inputCol="tokens_clean", outputCol="tf",
        numFeatures=num_features,
    )
    idf = IDF(inputCol="tf", outputCol="features")
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label_sent",
        maxIter=20,
        regParam=0.01,
        elasticNetParam=0.0,
        family="multinomial",
    )
    return Pipeline(stages=[label_idx, tokenizer, remover, hashing_tf, idf, lr])


def build_category_pipeline(num_features: int, num_trees: int) -> Pipeline:
    """Pipeline Random Forest pour la catégorisation des plaintes."""
    label_idx = StringIndexer(
        inputCol="categorie", outputCol="label_cat",
        handleInvalid="keep",
    )
    tokenizer = Tokenizer(inputCol="texte_clean", outputCol="tokens")
    remover = StopWordsRemover(inputCol="tokens", outputCol="tokens_clean")
    hashing_tf = HashingTF(
        inputCol="tokens_clean", outputCol="tf",
        numFeatures=num_features,
    )
    idf = IDF(inputCol="tf", outputCol="features")
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label_cat",
        numTrees=num_trees,
        maxDepth=6,
        seed=42,
    )
    return Pipeline(stages=[label_idx, tokenizer, remover, hashing_tf, idf, rf])


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    args = parse_args()
    metastore_uri = os.environ.get(
        "HIVE_METASTORE_URI", "thrift://hive-metastore:9083"
    )

    # ─── SparkSession avec Hive ──────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("VoxSN_TrainClassifier")
        .enableHiveSupport()
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.hive.metastore.uris", metastore_uri)
        .config("spark.sql.catalogImplementation", "hive")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ─── Chargement des données (Hive, fallback CSV) ─────────────────────────
    sql = f"""
        SELECT texte_clean,
               sentiment_label,
               categorie
        FROM {args.hive_database}.posts_analyses
        WHERE date_post >= DATE_SUB(CURRENT_DATE(), {args.window_days})
          AND texte_clean IS NOT NULL
          AND LENGTH(texte_clean) > 0
          AND sentiment_label IS NOT NULL
          AND categorie IS NOT NULL
    """
    logger.info("Chargement données — fenêtre %d jours", args.window_days)
    df = None
    try:
        df = spark.sql(sql).na.drop()
        total = df.count()
        logger.info("Lignes Hive utilisables : %d", total)
        if total < 100:
            df = None
    except Exception as exc:  # noqa: BLE001
        logger.warning("Lecture Hive impossible : %s", exc)

    if df is None:
        if not os.path.exists(args.csv_path):
            logger.error(
                "Données insuffisantes. Lancez : "
                "make hive-init && make hive-load-training "
                "ou : python scripts/seed_training_data.py"
            )
            spark.stop()
            return
        logger.info("Fallback CSV : %s", args.csv_path)
        df = (
            spark.read.option("header", True).csv(args.csv_path)
            .select("texte_clean", "sentiment_label", "categorie")
            .na.drop()
            .filter("LENGTH(texte_clean) > 0")
        )

    total = df.count()
    logger.info("Lignes utilisables : %d", total)
    if total < 100:
        logger.error(
            "Trop peu de données pour entraîner (min 100, trouvé %d). "
            "Lancez : make hive-init && make hive-load-training",
            total,
        )
        spark.stop()
        return

    # ─── Split 80/20 ─────────────────────────────────────────────────────────
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    logger.info("Train : %d | Test : %d", train_df.count(), test_df.count())

    # ─── Modèle 1 : sentiment ────────────────────────────────────────────────
    logger.info("Entraînement Logistic Regression (sentiment)...")
    pipeline_sent = build_sentiment_pipeline(args.num_features)
    model_sent = pipeline_sent.fit(train_df)

    # ─── Modèle 2 : catégorie ────────────────────────────────────────────────
    logger.info("Entraînement Random Forest (catégorie)...")
    pipeline_cat = build_category_pipeline(args.num_features, args.num_trees)
    model_cat = pipeline_cat.fit(train_df)

    # ─── Évaluation ──────────────────────────────────────────────────────────
    evaluator = MulticlassClassificationEvaluator(metricName="f1")

    preds_sent = model_sent.transform(test_df)
    evaluator.setLabelCol("label_sent").setPredictionCol("prediction")
    f1_sent = evaluator.evaluate(preds_sent)
    logger.info("[Sentiment] F1 = %.4f", f1_sent)

    preds_cat = model_cat.transform(test_df)
    evaluator.setLabelCol("label_cat").setPredictionCol("prediction")
    f1_cat = evaluator.evaluate(preds_cat)
    logger.info("[Catégorie] F1 = %.4f", f1_cat)

    # Métriques additionnelles : précision, recall
    evaluator.setMetricName("accuracy")
    acc_sent = evaluator.setLabelCol("label_sent").evaluate(preds_sent)
    acc_cat = evaluator.setLabelCol("label_cat").evaluate(preds_cat)
    logger.info("[Sentiment] Accuracy = %.4f", acc_sent)
    logger.info("[Catégorie] Accuracy = %.4f", acc_cat)

    # ─── Sauvegarde modèles ──────────────────────────────────────────────────
    os.makedirs(args.output_path, exist_ok=True)
    sentiment_path = f"{args.output_path}/sentiment"
    categorie_path = f"{args.output_path}/categorie"

    model_sent.write().overwrite().save(sentiment_path)
    model_cat.write().overwrite().save(categorie_path)
    logger.info("Modèles sauvegardés dans %s", args.output_path)

    # ─── Métadonnées ─────────────────────────────────────────────────────────
    metadata = {
        "trained_at": datetime.utcnow().isoformat() + "Z",
        "window_days": args.window_days,
        "n_samples": total,
        "f1_sentiment": float(f1_sent),
        "accuracy_sentiment": float(acc_sent),
        "f1_categorie": float(f1_cat),
        "accuracy_categorie": float(acc_cat),
        "num_features": args.num_features,
        "num_trees": args.num_trees,
        "sentiment_model_path": sentiment_path,
        "categorie_model_path": categorie_path,
    }
    with open(f"{args.output_path}/training_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    logger.info("Métadonnées : %s/training_metadata.json", args.output_path)

    # ─── MLflow tracking (optionnel) ─────────────────────────────────────────
    if args.mlflow_uri:
        try:
            import mlflow

            mlflow.set_tracking_uri(args.mlflow_uri)
            mlflow.set_experiment("vox_sn_classifier")
            with mlflow.start_run(run_name=f"train_{datetime.utcnow():%Y%m%d_%H%M%S}"):
                mlflow.log_params({
                    "window_days": args.window_days,
                    "num_features": args.num_features,
                    "num_trees": args.num_trees,
                })
                mlflow.log_metrics({
                    "f1_sentiment": f1_sent,
                    "f1_categorie": f1_cat,
                    "accuracy_sentiment": acc_sent,
                    "accuracy_categorie": acc_cat,
                    "n_samples": float(total),
                })
                mlflow.log_artifact(f"{args.output_path}/training_metadata.json")
            logger.info("Run MLflow enregistré sur %s", args.mlflow_uri)
        except Exception as exc:  # noqa: BLE001
            logger.warning("MLflow tracking échoué : %s", exc)

    spark.stop()
    logger.info("Entraînement terminé.")


if __name__ == "__main__":
    main()
