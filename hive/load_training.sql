-- Charge le CSV généré par scripts/seed_training_data.py dans posts_analyses
-- Usage (depuis l'hôte) :
--   make hive-load-training

USE vox_sn;

DROP TABLE IF EXISTS training_staging;

CREATE TABLE training_staging (
    post_id           STRING,
    service_cible     STRING,
    texte_clean       STRING,
    langue            STRING,
    canal             STRING,
    region            STRING,
    sentiment_score   STRING,
    sentiment_label   STRING,
    categorie         STRING,
    date_post         STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count" = "1");

LOAD DATA LOCAL INPATH '/tmp/training_data.csv' OVERWRITE INTO TABLE training_staging;

INSERT OVERWRITE TABLE posts_analyses PARTITION (date_post, service)
SELECT
    post_id,
    md5(post_id)                              AS citizen_id_secure,
    service_cible,
    texte_clean,
    langue,
    canal,
    region,
    CAST(sentiment_score AS FLOAT)            AS sentiment_score,
    sentiment_label,
    categorie,
    CASE
        WHEN CAST(sentiment_score AS FLOAT) < -0.5 THEN 'NEGATIF_FORT'
        ELSE 'NORMAL'
    END                                       AS statut_alerte,
    current_timestamp()                       AS ingestion_ts,
    date_post,
    service_cible                             AS service
FROM training_staging
WHERE texte_clean IS NOT NULL
  AND LENGTH(texte_clean) > 0;

DROP TABLE training_staging;

SELECT COUNT(*) AS nb_posts FROM posts_analyses;
