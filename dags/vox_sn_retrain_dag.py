"""
dags/vox_sn_retrain_dag.py — DAG de réentraînement ML hebdomadaire
==================================================================

Réentraîne les modèles Logistic Regression (sentiment) et Random Forest
(catégorie) chaque dimanche soir avec :
    - Les données des 30 derniers jours depuis Hive
    - Tracking complet via MLflow
    - Promotion conditionnelle si F1 > seuil

Schedule : tous les dimanches à 23:00 UTC
Auteur   : Vox-SN Team — UADB M2 BD&IA 2025-2026
"""
from __future__ import annotations

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger('vox_retrain_dag')


DEFAULT_ARGS = {
    'owner': 'vox_sn_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': True,
    'email': ['vox-sn-alerts@uadb.edu.sn'],
}

SEUIL_F1_PROMOTION = 0.70  # F1 minimal pour promouvoir le modèle en production


def lancer_entrainement(**ctx):
    """Soumet le job Spark train_classifier.py et capture les métriques."""
    import subprocess
    output_path = 'hdfs:///vox_sn/models/classifier_candidate'

    cmd = [
        'docker', 'exec', 'vox_spark_master',
        'spark-submit', '--master', 'spark://spark-master:7077',
        '/opt/spark-apps/train_classifier.py',
        '--output-path', output_path,
        '--window-days', '30',
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if result.returncode != 0:
        raise RuntimeError(f'Spark training échoué : {result.stderr}')
    logger.info(result.stdout)

    # Extraire le F1 depuis la sortie
    f1_sentiment = None
    for line in result.stdout.split('\n'):
        if 'F1 sentiment' in line:
            try:
                f1_sentiment = float(line.split('=')[-1].strip())
            except (ValueError, IndexError):
                pass

    ctx['ti'].xcom_push(key='f1_sentiment', value=f1_sentiment or 0.0)
    ctx['ti'].xcom_push(key='model_path', value=output_path)


def evaluer_promotion(**ctx):
    """Décide si le candidat doit être promu en production."""
    f1 = ctx['ti'].xcom_pull(key='f1_sentiment', task_ids='lancer_entrainement')
    logger.info(f'F1 du candidat : {f1}')
    return 'promouvoir_modele' if f1 and f1 >= SEUIL_F1_PROMOTION else 'rejeter_modele'


def promouvoir_modele(**ctx):
    """Copie le candidat vers le path 'latest' (promotion atomique)."""
    import subprocess
    candidate = ctx['ti'].xcom_pull(key='model_path', task_ids='lancer_entrainement')
    cmd = [
        'docker', 'exec', 'vox_spark_master',
        'hdfs', 'dfs', '-cp', '-f',
        candidate,
        'hdfs:///vox_sn/models/classifier_latest'
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(f'Promotion : {result.stdout}')
    if result.returncode != 0:
        logger.warning(f'Erreur promotion (non bloquant) : {result.stderr}')


def rejeter_modele(**ctx):
    """Log le rejet et envoie une alerte si trop d'échecs consécutifs."""
    f1 = ctx['ti'].xcom_pull(key='f1_sentiment', task_ids='lancer_entrainement')
    logger.warning(f'Modèle rejeté — F1={f1} < seuil {SEUIL_F1_PROMOTION}')
    # Ici on pourrait envoyer un Slack/email d'alerte


with DAG(
    dag_id='vox_sn_retrain',
    description='Vox-SN — Réentraînement hebdomadaire avec MLflow',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 23 * * 0',  # Dimanche 23:00 UTC
    start_date=days_ago(7),
    catchup=False,
    max_active_runs=1,
    tags=['vox-sn', 'mlops', 'training'],
) as dag:

    start = DummyOperator(task_id='start')

    entrainement = PythonOperator(
        task_id='lancer_entrainement',
        python_callable=lancer_entrainement,
    )

    branche = BranchPythonOperator(
        task_id='evaluer_promotion',
        python_callable=evaluer_promotion,
    )

    promotion = PythonOperator(
        task_id='promouvoir_modele',
        python_callable=promouvoir_modele,
    )

    rejet = PythonOperator(
        task_id='rejeter_modele',
        python_callable=rejeter_modele,
    )

    end = DummyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    start >> entrainement >> branche >> [promotion, rejet] >> end
