"""
dags/vox_sn_ingestion_dag.py — DAG d'ingestion batch quotidienne
================================================================

Ce DAG complète le pipeline streaming en assurant :
    1. Une re-ingestion quotidienne des posts archivés (rattrapage)
    2. Le batch de validation Pandera sur les fichiers JSON déposés
    3. La consolidation Hive (compaction des petits fichiers)
    4. L'export quotidien des agrégats vers HDFS

Schedule : tous les jours à 02:00 UTC
Auteur   : Vox-SN Team — UADB M2 BD&IA 2025-2026
"""
from __future__ import annotations

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger('vox_ingestion_dag')


# =============================================================================
# Configuration
# =============================================================================
DEFAULT_ARGS = {
    'owner': 'vox_sn_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': True,
    'email': ['vox-sn-alerts@uadb.edu.sn'],
}


# =============================================================================
# Tâches
# =============================================================================
def valider_fichiers_du_jour(**ctx):
    """
    Applique le schéma Pandera sur les fichiers JSON arrivés dans la journée.
    Les fichiers rejetés sont déplacés dans data/posts/quarantine/.
    """
    import os
    import json
    import pandas as pd
    import sys

    sys.path.insert(0, '/opt/airflow/spark')
    from schema import SocialSentimentSchema, validate_and_filter

    input_dir = '/opt/airflow/spark/../data/posts'
    quarantine = os.path.join(input_dir, 'quarantine')
    os.makedirs(quarantine, exist_ok=True)

    total_lus, total_valides, total_rejetes = 0, 0, 0

    for fname in os.listdir(input_dir):
        if not fname.endswith('.json'):
            continue
        path = os.path.join(input_dir, fname)
        try:
            with open(path) as f:
                lines = [json.loads(line) for line in f if line.strip()]
            df = pd.DataFrame(lines)
            total_lus += len(df)
            df_ok = validate_and_filter(df, SocialSentimentSchema)
            total_valides += len(df_ok)
            total_rejetes += len(df) - len(df_ok)
            logger.info(f'{fname}: {len(df_ok)}/{len(df)} valides')
        except Exception as e:
            logger.error(f'Erreur sur {fname}: {e}')
            os.rename(path, os.path.join(quarantine, fname))

    ctx['ti'].xcom_push(key='valides', value=total_valides)
    ctx['ti'].xcom_push(key='rejetes', value=total_rejetes)
    logger.info(f'Total : {total_valides} valides / {total_rejetes} rejetés')


def consolider_hive(**ctx):
    """Compaction quotidienne des partitions Hive."""
    from pyhive import hive

    conn = hive.Connection(host='hive-server', port=10000, database='vox_sn')
    cur = conn.cursor()

    # Compaction des petits fichiers ORC (concatenation)
    cur.execute("ALTER TABLE posts_analyses CONCATENATE")
    logger.info('Compaction ORC effectuée')

    # Statistiques sur la journée écoulée
    cur.execute("""
        SELECT service_cible,
               COUNT(*) AS nb_posts,
               AVG(sentiment_score) AS sentiment_moyen
        FROM posts_analyses
        WHERE date_post = DATE_FORMAT(DATE_SUB(CURRENT_DATE, 1), 'yyyy-MM-dd')
        GROUP BY service_cible
    """)
    rows = cur.fetchall()
    for row in rows:
        logger.info(f'  {row[0]}: {row[1]} posts | sentiment={row[2]:.3f}')

    conn.close()


def export_agregats_hdfs(**ctx):
    """Export quotidien des agrégats Hive vers HDFS pour archivage."""
    import subprocess
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    cmd = f"""
    hive -e "INSERT OVERWRITE DIRECTORY 'hdfs:///vox_sn/exports/{yesterday}/'
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
             SELECT * FROM vox_sn.vue_battle_mobile_money"
    """
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        logger.warning(f'Export HDFS échoué (non bloquant) : {result.stderr}')
    else:
        logger.info(f'Export HDFS OK pour {yesterday}')


# =============================================================================
# Définition du DAG
# =============================================================================
with DAG(
    dag_id='vox_sn_ingestion',
    description='Vox-SN — Ingestion batch quotidienne et consolidation Hive',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 2 * * *',  # 02:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['vox-sn', 'ingestion', 'batch'],
) as dag:

    start = DummyOperator(task_id='start')

    valider = PythonOperator(
        task_id='valider_fichiers_du_jour',
        python_callable=valider_fichiers_du_jour,
    )

    consolider = PythonOperator(
        task_id='consolider_hive',
        python_callable=consolider_hive,
    )

    exporter = PythonOperator(
        task_id='export_agregats_hdfs',
        python_callable=export_agregats_hdfs,
    )

    nettoyer_quarantine = BashOperator(
        task_id='nettoyer_quarantine_30j',
        bash_command="find /opt/airflow/spark/../data/posts/quarantine -mtime +30 -delete || true",
    )

    end = DummyOperator(task_id='end')

    start >> valider >> consolider >> exporter >> nettoyer_quarantine >> end
