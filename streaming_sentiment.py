# streaming_sentiment.py — Pipeline NLP Spark Streaming Vox-SN
# Privacy + Nettoyage Wolof/FR + Scoring sentiment + Classification
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sha2, concat, lit, from_json, to_json, struct,
    lower, regexp_replace, trim, split, array_contains,
    when, current_timestamp, coalesce, udf, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType
)
from pyspark.sql.types import StringType as ST
import json

SALT = os.environ.get('CITIZEN_SECRET_SALT', 'UADB_VOX_2025')
BROKERS = 'localhost:9093'  # CORRECTION : localhost au lieu de kafka (hôte Docker)

spark = SparkSession.builder \
    .appName('Vox_SN_NLP') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

post_schema = StructType([
    StructField('post_id',       StringType(), True),
    StructField('user_id',       StringType(), True),
    StructField('phone_number',  StringType(), True),
    StructField('service_cible', StringType(), True),
    StructField('texte_du_post', StringType(), True),
    StructField('langue',        StringType(), True),
    StructField('canal',         StringType(), True),
    StructField('timestamp',     StringType(), True),
    StructField('region',        StringType(), True),
])

from lexique_sn import NEGATIF, POSITIF, CATEGORIES, ALL_STOPWORDS

bc_negatif    = spark.sparkContext.broadcast(NEGATIF)
bc_positif    = spark.sparkContext.broadcast(POSITIF)
bc_categories = spark.sparkContext.broadcast(CATEGORIES)
bc_stopwords  = spark.sparkContext.broadcast(ALL_STOPWORDS)

@udf(returnType=FloatType())
def score_sentiment(texte: str) -> float:
    if not texte:
        return 0.0
    texte_lower = texte.lower()
    score = 0.0
    count = 0
    for terme, val in bc_negatif.value.items():
        if terme in texte_lower:
            score += val; count += 1
    for terme, val in bc_positif.value.items():
        if terme in texte_lower:
            score += val; count += 1
    return float(score / max(count, 1))

@udf(returnType=StringType())
def categoriser(texte: str) -> str:
    if not texte:
        return 'INCONNU'
    texte_lower = texte.lower()
    scores = {}
    for cat, mots in bc_categories.value.items():
        scores[cat] = sum(1 for m in mots if m in texte_lower)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else 'AUTRE'

@udf(returnType=StringType())
def nettoyer_texte(texte: str) -> str:
    if not texte:
        return ''
    tokens = texte.lower().split()
    tokens = [t for t in tokens if t not in bc_stopwords.value
              and len(t) > 2 and t.isalpha()]
    return ' '.join(tokens)

raw_df = (spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', BROKERS)
    .option('subscribe', 'social_raw')
    .option('startingOffsets', 'latest')
    .load()
    .select(from_json(col('value').cast('string'), post_schema).alias('d'))
    .select('d.*')
    .withColumn('event_ts', current_timestamp())
)

secure_df = (raw_df
    .withColumn('citizen_id_secure',
                sha2(concat(col('user_id'), lit(SALT)), 256))
    .drop('user_id', 'phone_number')
)

clean_df = (secure_df
    .withColumn('texte_norm',
                trim(regexp_replace(
                    lower(col('texte_du_post')),
                    r'[^a-zàáâäèéêëîïôùûüœç\s]', ' '
                )))
)

nlp_df = (clean_df
    .withColumn('texte_clean',     nettoyer_texte(col('texte_norm')))
    .withColumn('sentiment_score', score_sentiment(col('texte_norm')))
    .withColumn('categorie',       categoriser(col('texte_norm')))
    .withColumn('sentiment_label',
        when(col('sentiment_score') < -0.5, lit('NEGATIF_FORT'))
        .when(col('sentiment_score') < 0.0,  lit('NEGATIF'))
        .when(col('sentiment_score') > 0.3,  lit('POSITIF'))
        .otherwise(lit('NEUTRE')))
    .withColumn('statut_alerte',
        when((col('sentiment_score') < -0.5) &
             (col('categorie').isin('FRAUDE','TECHNIQUE')), lit('CRISE'))
        .when(col('sentiment_score') < -0.5, lit('NEGATIF_FORT'))
        .otherwise(lit('NORMAL')))
)

sentiment_window = (nlp_df
    .withWatermark('event_ts', '10 minutes')
    .groupBy(window('event_ts', '1 hour', '15 minutes'), 'service_cible')
    .agg({'sentiment_score': 'avg', 'post_id': 'count'})
    .withColumnRenamed('avg(sentiment_score)', 'sentiment_moyen')
    .withColumnRenamed('count(post_id)', 'nb_posts')
    .withColumn('statut_operateur',
        when(col('sentiment_moyen') < -0.5, lit('CRISE'))
        .when(col('sentiment_moyen') < -0.2, lit('ATTENTION'))
        .otherwise(lit('NORMAL')))
)

q1 = (nlp_df
    .select(to_json(struct('*')).alias('value'))
    .writeStream.format('kafka')
    .option('kafka.bootstrap.servers', BROKERS)
    .option('topic', 'social_analyzed')
    .option('checkpointLocation', '/tmp/vox_posts_ckpt')
    .outputMode('append').start()
)

q2 = (sentiment_window
    .select(to_json(struct('*')).alias('value'))
    .writeStream.format('kafka')
    .option('kafka.bootstrap.servers', BROKERS)
    .option('topic', 'social_sentiment_agg')
    .option('checkpointLocation', '/tmp/vox_agg_ckpt')
    .outputMode('update').start()
)

q1.awaitTermination()