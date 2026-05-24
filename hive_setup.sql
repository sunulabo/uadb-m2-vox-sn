-- hive_setup.sql — Tables et vues Hive Vox-SN
-- Parts de voix, tendances sentiment, comparaison opérateurs Mobile Money
-- Auteur : Projet Vox-SN | UADB Master 2 Big Data & IA 2025-2026

-- ── Création de la base de données ───────────────────────────────────────
-- Base dédiée à l'analyse de sentiment des services publics et Fintech
CREATE DATABASE IF NOT EXISTS vox_sn
    COMMENT 'Analyse sentiment services publics et Fintech — UADB 2025';

USE vox_sn;

-- ── Table principale des posts analysés ──────────────────────────────────
-- Stocke les posts après traitement NLP par Spark Streaming
-- Partitionnée par date et service pour optimiser les requêtes
-- citizen_id_secure : SHA-256 du user_id — jamais de PII brut
CREATE TABLE IF NOT EXISTS posts_analyses (
    post_id           STRING,
    citizen_id_secure STRING COMMENT 'SHA-256 — jamais user_id brut',
    service_cible     STRING,
    texte_clean       STRING,
    langue            STRING,
    canal             STRING,
    region            STRING,
    sentiment_score   FLOAT,
    sentiment_label   STRING,
    categorie         STRING,
    statut_alerte     STRING,
    ingestion_ts      TIMESTAMP
)
PARTITIONED BY (date_post STRING, service STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ── Table agrégats horaires ───────────────────────────────────────────────
-- Alimentée toutes les heures par le DAG Airflow vox_sn_monitoring
-- Contient le sentiment moyen, nb fraudes et pannes par opérateur
CREATE TABLE IF NOT EXISTS sentiment_hourly (
    service_cible   STRING,
    heure           TIMESTAMP,
    nb_posts        INT,
    sentiment_moyen FLOAT,
    nb_fraudes      INT,
    nb_pannes       INT,
    statut          STRING   -- CRISE / ATTENTION / NORMAL
)
STORED AS ORC;

-- ── Vue : Battle des Mobile Money (Wave vs Orange Money vs Free Money) ────
-- Compare les 3 opérateurs sur 7 jours glissants
-- Métriques : sentiment moyen, % positif, % critique, fraudes, pannes
CREATE OR REPLACE VIEW vue_battle_mobile_money AS
SELECT
    service_cible,
    COUNT(*)                                                        AS total_mentions,
    AVG(COALESCE(sentiment_score, 0))                               AS sentiment_moyen,
    SUM(CASE WHEN sentiment_label='POSITIF' THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*)                                          AS pct_positif,
    SUM(CASE WHEN sentiment_label='NEGATIF_FORT' THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*)                                          AS pct_critique,
    SUM(CASE WHEN categorie='FRAUDE' THEN 1 ELSE 0 END)            AS nb_fraudes,
    SUM(CASE WHEN categorie='TARIF' THEN 1 ELSE 0 END)             AS nb_plaintes_tarif,
    SUM(CASE WHEN categorie='TECHNIQUE' THEN 1 ELSE 0 END)         AS nb_pannes
FROM posts_analyses
WHERE service_cible IN ('WAVE','ORANGE_MONEY','FREE_MONEY')
  AND date_post >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY service_cible
ORDER BY sentiment_moyen DESC;

-- ── Vue : Parts de voix par service (tous services) ──────────────────────
-- Calcule la part de mention de chaque service sur 30 jours
-- nps_approx : approximation du Net Promoter Score via sentiment moyen
CREATE OR REPLACE VIEW vue_parts_de_voix AS
SELECT
    service_cible,
    COUNT(*)                                          AS mentions,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()         AS part_de_voix_pct,
    AVG(COALESCE(sentiment_score, 0))                 AS nps_approx
FROM posts_analyses
WHERE date_post >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY service_cible
ORDER BY mentions DESC;

-- ── Vue : Détection de crises confirmées ─────────────────────────────────
-- Détecte les crises actives sur la dernière heure
-- Seuil : sentiment < -0.5 ET minimum 5 posts négatifs = crise confirmée
CREATE OR REPLACE VIEW vue_alertes_crises AS
SELECT
    service_cible,
    categorie,
    COUNT(*)                              AS nb_posts_negatifs,
    AVG(COALESCE(sentiment_score, 0))     AS sentiment_moyen,
    MIN(ingestion_ts)                     AS debut_crise,
    MAX(ingestion_ts)                     AS derniere_maj
FROM posts_analyses
WHERE statut_alerte = 'CRISE'
--   AND ingestion_ts >= DATE_SUB(CURRENT_TIMESTAMP, 1/24.0)
AND ingestion_ts >= DATE_SUB(CURRENT_TIMESTAMP, 1)
GROUP BY service_cible, categorie
HAVING COUNT(*) >= 5  -- Min 5 posts négatifs = crise confirmée
ORDER BY nb_posts_negatifs DESC;