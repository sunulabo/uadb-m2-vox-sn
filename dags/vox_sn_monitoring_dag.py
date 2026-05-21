"""
vox_sn_monitoring_dag.py — DAG Airflow Vox-SN
==============================================

Orchestration MLOps Vox-SN. Cycle de vie automatisé :

    [start] ─► [recalculate_sentiment] ─► [detect_crises] ─┬─► [trigger_retrain] ─► [end]
                                                            └─► [skip_retrain] ───────┘

Schedule  : toutes les heures (`0 * * * *`)
Tasks     :
    T1 — recalculate_sentiment : INSERT OVERWRITE vox_sn.sentiment_hourly
    T2 — detect_crises         : Branch — > 3 crises = trigger retrain
    T3 — trigger_retrain       : spark-submit train_classifier.py
    T4 — alert_crisis          : envoi alertes (placeholder)

Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
Auteur    : Vox-SN Team - UADB M2 BD&IA 2025-2026
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago


# =============================================================================
# Configuration
# =============================================================================
logger = logging.getLogger("vox_sn_dag")

DEFAULT_ARGS = {
    "owner": "vox-sn-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["vox-sn@uadb.sn"],
}

# Seuil de déclenchement du retrain
CRISIS_THRESHOLD_TO_RETRAIN = 3


# =============================================================================
# Callbacks
# =============================================================================
def recalculate_sentiment(**context) -> None:
    """T1 — Recalcule les agrégats horaires dans Hive."""
    from pyhive import hive

    logger.info("T1 — Recalcul sentiment horaire...")
    conn = hive.Connection(host="hive-metastore", port=10000, database="vox_sn")
    cursor = conn.cursor()
    try:
        sql = """
            INSERT OVERWRITE TABLE vox_sn.sentiment_hourly
            SELECT
                service_cible,
                DATE_TRUNC('HOUR', CURRENT_TIMESTAMP()) AS heure,
                COUNT(*) AS nb_posts,
                AVG(COALESCE(sentiment_score, 0)) AS sentiment_moyen,
                SUM(CASE WHEN categorie = 'FRAUDE' THEN 1 ELSE 0 END) AS nb_fraudes,
                SUM(CASE WHEN categorie = 'TECHNIQUE' THEN 1 ELSE 0 END) AS nb_pannes,
                SUM(CASE WHEN categorie = 'TARIF' THEN 1 ELSE 0 END) AS nb_tarif,
                CASE
                    WHEN AVG(COALESCE(sentiment_score, 0)) < -0.5 THEN 'CRISE'
                    WHEN AVG(COALESCE(sentiment_score, 0)) < -0.2 THEN 'ATTENTION'
                    ELSE 'NORMAL'
                END AS statut
            FROM vox_sn.posts_analyses
            WHERE date_post >= DATE_SUB(CURRENT_DATE(), 1)
            GROUP BY service_cible
        """
        cursor.execute(sql)
        logger.info("✅ Sentiment horaire recalculé.")
    finally:
        cursor.close()
        conn.close()


def detect_crises(**context) -> str:
    """T2 — Détecte les crises actives, branche selon le nombre."""
    from pyhive import hive

    logger.info("T2 — Détection des crises actives...")
    conn = hive.Connection(host="hive-metastore", port=10000, database="vox_sn")
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT service_cible, sentiment_moyen, nb_fraudes, nb_pannes, statut
            FROM vox_sn.sentiment_hourly
            WHERE statut IN ('CRISE', 'ATTENTION')
        """)
        crises = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    if not crises:
        logger.info("Aucune crise détectée.")
        context["ti"].xcom_push(key="nb_crises", value=0)
        return "skip_retrain"

    for service, score, fraudes, pannes, statut in crises:
        logger.warning(
            "⚠️  %s : score=%.3f | fraudes=%d | pannes=%d | statut=%s",
            service, score, fraudes, pannes, statut,
        )

    n_crises = len([c for c in crises if c[-1] == "CRISE"])
    context["ti"].xcom_push(key="nb_crises", value=n_crises)
    return "trigger_retrain" if n_crises > CRISIS_THRESHOLD_TO_RETRAIN else "skip_retrain"


def trigger_retrain(**context) -> None:
    """T3 — Lance le réentraînement Spark MLlib."""
    logger.info("T3 — Réentraînement des modèles...")
    cmd = [
        "spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "/opt/airflow/spark/train_classifier.py",
        "--output-path", "/opt/airflow/models/classifier_latest",
        "--window-days", "30",
        "--mlflow-uri", "http://mlflow:5000",
    ]
    logger.info("Commande : %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if result.returncode != 0:
        logger.error("Spark échec : %s", result.stderr[-2000:])
        raise RuntimeError(f"spark-submit retour {result.returncode}")
    logger.info("✅ Modèles réentraînés.")
    logger.debug("Stdout : %s", result.stdout[-1500:])


def alert_crisis(**context) -> None:
    """T4 — Envoi des alertes (slack/email/HBase) — placeholder simple."""
    nb_crises = context["ti"].xcom_pull(key="nb_crises", task_ids="detect_crises") or 0
    logger.warning(
        "🚨 ALERTE CRISE — %d service(s) en CRISE. "
        "TODO : intégrer webhook Slack/SMS Sonatel.",
        nb_crises,
    )


# =============================================================================
# Construction du DAG
# =============================================================================
with DAG(
    dag_id="vox_sn_monitoring",
    default_args=DEFAULT_ARGS,
    description="Monitoring sentiment Vox-SN — recalcul horaire + retrain conditionnel",
    schedule_interval="0 * * * *",     # Chaque heure
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["vox-sn", "nlp", "fintech", "uadb"],
) as dag:

    start = EmptyOperator(task_id="start")
    skip = EmptyOperator(task_id="skip_retrain")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    t1 = PythonOperator(
        task_id="recalculate_sentiment",
        python_callable=recalculate_sentiment,
    )

    t2 = BranchPythonOperator(
        task_id="detect_crises",
        python_callable=detect_crises,
    )

    t3 = PythonOperator(
        task_id="trigger_retrain",
        python_callable=trigger_retrain,
    )

    t4 = PythonOperator(
        task_id="alert_crisis",
        python_callable=alert_crisis,
        trigger_rule="all_done",
    )

    # Graph
    start >> t1 >> t2
    t2 >> [t3, skip]
    [t3, skip] >> t4 >> end
