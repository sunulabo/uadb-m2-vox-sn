# dags/vox_sn_monitoring_dag.py — DAG Airflow Vox-SN
# Recalcul sentiment toutes les heures + réentraînement conditionnel
# Déclenché chaque heure via schedule_interval='0 * * * *'

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess, logging

logger = logging.getLogger('vox_dag')

# ── Configuration par défaut du DAG ──────────────────────────────────────
# Propriétaire, nombre de tentatives, délai entre tentatives
default_args = {
    'owner': 'seye_ahmed',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,  # Activer en production avec serveur mail
    'email': ['vox-sn@sonatel.sn']
}

# ── T1 : Recalcul sentiment par opérateur (toutes les heures) ────────────
# Lit les posts analysés depuis Hive et calcule le sentiment moyen par service
# Détecte automatiquement les statuts CRISE / ATTENTION / NORMAL
def recalculate_sentiment(**ctx):
    from pyhive import hive
    # Connexion au metastore Hive (localhost depuis l'hôte Docker)
    conn = hive.Connection(host='localhost', port=10000)
    cursor = conn.cursor()
    # Insertion des agrégats horaires dans la table sentiment_hourly
    cursor.execute("""
        INSERT OVERWRITE TABLE vox_sn.sentiment_hourly
        SELECT
            service_cible,
            DATE_TRUNC('hour', CURRENT_TIMESTAMP) AS heure,
            COUNT(*) AS nb_posts,
            AVG(sentiment_score) AS sentiment_moyen,
            SUM(CASE WHEN categorie='FRAUDE' THEN 1 ELSE 0 END) AS nb_fraudes,
            SUM(CASE WHEN categorie='TECHNIQUE' THEN 1 ELSE 0 END) AS nb_pannes,
            CASE
                WHEN AVG(sentiment_score) < -0.5 THEN 'CRISE'
                WHEN AVG(sentiment_score) < -0.2 THEN 'ATTENTION'
                ELSE 'NORMAL'
            END AS statut
        FROM vox_sn.posts_analyses
        WHERE date_post >= DATE_SUB(CURRENT_DATE, 1)
        GROUP BY service_cible
    """)
    logger.info('Sentiment horaire recalculé avec succès')

# ── T2 : Détection et alerte de crise ────────────────────────────────────
# BranchPythonOperator : décide si le réentraînement est nécessaire
# Si plus de 3 opérateurs en CRISE → trigger_retrain, sinon skip_retrain
def detect_crises(**ctx):
    from pyhive import hive
    conn = hive.Connection(host='localhost', port=10000)
    cursor = conn.cursor()
    # Récupère les opérateurs en état CRISE ou ATTENTION
    cursor.execute("""
        SELECT service_cible, sentiment_moyen, nb_fraudes, statut
        FROM vox_sn.sentiment_hourly
        WHERE statut IN ('CRISE','ATTENTION')
    """)
    crises = cursor.fetchall()
    # Log de chaque crise détectée
    if crises:
        for row in crises:
            logger.warning(f'CRISE DÉTECTÉE : {row[0]} | score={row[1]:.2f}')
    nb = len(crises) if crises else 0
    # Pousse le nombre de crises dans XCom pour traçabilité
    ctx['ti'].xcom_push(key='nb_crises', value=nb)
    # Branchement : réentraîner si plus de 3 crises simultanées
    return 'trigger_retrain' if nb > 3 else 'skip_retrain'

# ── T3 : Réentraînement conditionnel du modèle NLP ───────────────────────
# Déclenché uniquement si detect_crises retourne 'trigger_retrain'
# Lance train_classifier.py avec les 30 derniers jours de données
def trigger_retrain(**ctx):
    result = subprocess.run([
        'python', '/home/abasse/vox-sn/train_classifier.py',
        '--output-path', '/home/abasse/vox-sn/models/classifier_latest',
        '--window-days', '30'
    ], capture_output=True, text=True, timeout=3600)
    # Vérification du code de retour du job
    if result.returncode != 0:
        raise Exception(f'Train job failed: {result.stderr}')
    logger.info('Modèle NLP mis à jour avec succès')

# ── Définition du DAG et ordonnancement des tâches ───────────────────────
with DAG(
    'vox_sn_monitoring',
    default_args=default_args,
    description='Monitoring sentiment Vox-SN — recalcul horaire',
    schedule_interval='0 * * * *',  # Déclenchement toutes les heures
    start_date=days_ago(1),
    catchup=False,  # Ne pas rejouer les runs passés
    tags=['vox-sn', 'nlp', 'fintech']
) as dag:

    # Tâche de démarrage (point d'entrée du DAG)
    start = DummyOperator(task_id='start')

    # Tâche de fin sans réentraînement
    skip  = DummyOperator(task_id='skip_retrain')

    # Tâche de fin après réentraînement
    end   = DummyOperator(task_id='end')

    # T1 : Recalcul du sentiment horaire
    t1 = PythonOperator(
        task_id='recalculate_sentiment',
        python_callable=recalculate_sentiment,
        provide_context=True
    )

    # T2 : Détection de crises + branchement conditionnel
    branch = BranchPythonOperator(
        task_id='detect_crises',
        python_callable=detect_crises,
        provide_context=True
    )

    # T3 : Réentraînement du modèle si nécessaire
    t3 = PythonOperator(
        task_id='trigger_retrain',
        python_callable=trigger_retrain,
        provide_context=True
    )

    # Ordonnancement : start → recalcul → détection → [retrain OU skip] → end
    start >> t1 >> branch >> [t3, skip]
    t3 >> end
    skip >> end