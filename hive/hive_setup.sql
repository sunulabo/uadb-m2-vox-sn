-- =============================================================================
-- Vox-SN — hive_setup.sql
-- =============================================================================
-- Schéma Hive complet : base, tables, vues analytiques.
-- À exécuter dans Beeline :
--   beeline -u jdbc:hive2://localhost:10000 -f hive_setup.sql
--
-- Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Base de données
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS vox_sn
    COMMENT 'Analyse sentiment services publics & Fintech — UADB 2025'
    WITH DBPROPERTIES (
        'creator' = 'vox-sn-team',
        'created_at' = '2025-11-01'
    );

USE vox_sn;


-- -----------------------------------------------------------------------------
-- 2. Table principale : posts_analyses
-- Partitionnée par date_post + service pour les scans efficaces.
-- Format ORC + compression SNAPPY (analytique).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS posts_analyses (
    post_id             STRING                COMMENT 'UUID du post',
    citizen_id_secure   STRING                COMMENT 'SHA-256 — jamais user_id brut',
    service_cible       STRING                COMMENT 'WAVE / OM / FREE / SENELEC / SEN_EAU / TER',
    texte_clean         STRING                COMMENT 'Texte nettoyé (sans stopwords)',
    langue              STRING                COMMENT 'FR / WO / EN',
    canal               STRING                COMMENT 'TWITTER / FACEBOOK / WHATSAPP / RECLAMATION',
    region              STRING                COMMENT 'Région SN',
    sentiment_score     FLOAT                 COMMENT 'Score lexical ∈ [-1, +1]',
    sentiment_label     STRING                COMMENT 'NEGATIF_FORT / NEGATIF / NEUTRE / POSITIF',
    categorie           STRING                COMMENT 'TARIF / TECHNIQUE / FRAUDE / SERVICE_CLIENT / AUTRE',
    statut_alerte       STRING                COMMENT 'CRISE / NEGATIF_FORT / NORMAL',
    ingestion_ts        TIMESTAMP             COMMENT 'Timestamp ingestion Spark'
)
PARTITIONED BY (
    date_post STRING,
    service STRING
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'orc.bloom.filter.columns' = 'post_id, service_cible, statut_alerte'
);


-- -----------------------------------------------------------------------------
-- 3. Table agrégats horaires : sentiment_hourly
-- Alimentée par le DAG Airflow vox_sn_monitoring (T1 recalculate_sentiment)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sentiment_hourly (
    service_cible      STRING,
    heure              TIMESTAMP,
    nb_posts           INT,
    sentiment_moyen    FLOAT,
    nb_fraudes         INT,
    nb_pannes          INT,
    nb_tarif           INT,
    statut             STRING                 COMMENT 'CRISE / ATTENTION / NORMAL'
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- -----------------------------------------------------------------------------
-- 4. Table alertes (mirroring HBase pour analyse historique)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alertes_history (
    alerte_id          STRING,
    service_cible      STRING,
    categorie          STRING,
    sentiment_moyen    FLOAT,
    nb_posts_negatifs  INT,
    debut_crise        TIMESTAMP,
    fin_crise          TIMESTAMP,
    resolue            BOOLEAN,
    cree_le            TIMESTAMP
)
STORED AS ORC;


-- =============================================================================
-- 5. VUES ANALYTIQUES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Vue : Battle Mobile Money (Wave vs OM vs Free Money) — 7 jours glissants
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vue_battle_mobile_money AS
SELECT
    service_cible,
    COUNT(*) AS total_mentions,
    ROUND(AVG(COALESCE(sentiment_score, 0)), 3) AS sentiment_moyen,
    ROUND(
        SUM(CASE WHEN sentiment_label = 'POSITIF' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS pct_positif,
    ROUND(
        SUM(CASE WHEN sentiment_label = 'NEGATIF_FORT' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS pct_critique,
    SUM(CASE WHEN categorie = 'FRAUDE' THEN 1 ELSE 0 END) AS nb_fraudes,
    SUM(CASE WHEN categorie = 'TARIF' THEN 1 ELSE 0 END) AS nb_plaintes_tarif,
    SUM(CASE WHEN categorie = 'TECHNIQUE' THEN 1 ELSE 0 END) AS nb_pannes,
    SUM(CASE WHEN statut_alerte = 'CRISE' THEN 1 ELSE 0 END) AS nb_crises
FROM posts_analyses
WHERE service_cible IN ('WAVE', 'ORANGE_MONEY', 'FREE_MONEY')
  AND date_post >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY service_cible
ORDER BY sentiment_moyen DESC;


-- -----------------------------------------------------------------------------
-- Vue : Parts de voix par service (30 jours glissants)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vue_parts_de_voix AS
SELECT
    service_cible,
    COUNT(*) AS mentions,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2
    ) AS part_de_voix_pct,
    ROUND(AVG(COALESCE(sentiment_score, 0)), 3) AS nps_approx,
    SUM(CASE WHEN langue = 'WO' THEN 1 ELSE 0 END) AS mentions_wolof,
    SUM(CASE WHEN langue = 'FR' THEN 1 ELSE 0 END) AS mentions_fr,
    SUM(CASE WHEN langue = 'EN' THEN 1 ELSE 0 END) AS mentions_en
FROM posts_analyses
WHERE date_post >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY service_cible
ORDER BY mentions DESC;


-- -----------------------------------------------------------------------------
-- Vue : Alertes de crise actives (1h glissante, min 5 posts négatifs)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vue_alertes_crises AS
SELECT
    service_cible,
    categorie,
    COUNT(*) AS nb_posts_negatifs,
    ROUND(AVG(COALESCE(sentiment_score, 0)), 3) AS sentiment_moyen,
    MIN(ingestion_ts) AS debut_crise,
    MAX(ingestion_ts) AS derniere_maj,
    COLLECT_SET(region) AS regions_touchees
FROM posts_analyses
WHERE statut_alerte = 'CRISE'
  AND ingestion_ts >= FROM_UNIXTIME(UNIX_TIMESTAMP() - 3600)
GROUP BY service_cible, categorie
HAVING COUNT(*) >= 5
ORDER BY nb_posts_negatifs DESC;


-- -----------------------------------------------------------------------------
-- Vue : Tendances temporelles (par jour)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vue_tendances_temporelles AS
SELECT
    date_post,
    service_cible,
    COUNT(*) AS nb_posts,
    ROUND(AVG(COALESCE(sentiment_score, 0)), 3) AS sentiment_jour,
    SUM(CASE WHEN statut_alerte = 'CRISE' THEN 1 ELSE 0 END) AS nb_crises_jour
FROM posts_analyses
WHERE date_post >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY date_post, service_cible
ORDER BY date_post DESC, service_cible;


-- -----------------------------------------------------------------------------
-- Vue : Catégories de plaintes par service (camembert)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vue_categories_plaintes AS
SELECT
    service_cible,
    categorie,
    COUNT(*) AS nb,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY service_cible), 2
    ) AS pct_du_service
FROM posts_analyses
WHERE date_post >= DATE_SUB(CURRENT_DATE(), 7)
  AND categorie NOT IN ('AUTRE', 'INCONNU')
GROUP BY service_cible, categorie
ORDER BY service_cible, nb DESC;


-- -----------------------------------------------------------------------------
-- Vue : Top régions négatives
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vue_top_regions_negatives AS
SELECT
    region,
    COUNT(*) AS nb_posts,
    ROUND(AVG(sentiment_score), 3) AS sentiment_moyen,
    SUM(CASE WHEN statut_alerte = 'CRISE' THEN 1 ELSE 0 END) AS nb_crises
FROM posts_analyses
WHERE date_post >= DATE_SUB(CURRENT_DATE(), 7)
  AND sentiment_score < 0
GROUP BY region
ORDER BY sentiment_moyen ASC, nb_posts DESC;


-- =============================================================================
-- 6. EXEMPLES DE REQUÊTES (pour la soutenance)
-- =============================================================================

-- Battle Mobile Money :
-- SELECT * FROM vue_battle_mobile_money;

-- Crises actives :
-- SELECT * FROM vue_alertes_crises;

-- Top 10 régions à problèmes :
-- SELECT * FROM vue_top_regions_negatives LIMIT 10;

-- Évolution Wave sur 30 jours :
-- SELECT date_post, sentiment_jour, nb_crises_jour
-- FROM vue_tendances_temporelles
-- WHERE service_cible = 'WAVE'
-- ORDER BY date_post;

SHOW TABLES;
SHOW VIEWS;
