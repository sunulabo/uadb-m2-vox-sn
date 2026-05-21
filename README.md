# Vox-SN — Analyse de Sentiment Services Publics & Fintech

![Status](https://img.shields.io/badge/status-production--ready-success)
![Python](https://img.shields.io/badge/python-3.9-blue)
![Spark](https://img.shields.io/badge/spark-3.3.2-orange)
![Kafka](https://img.shields.io/badge/kafka-3.3-red)
![License](https://img.shields.io/badge/license-Academic-purple)

> **Master 2 Big Data & Intelligence Artificielle — UADB 2025-2026**
> Plateforme Big Data temps réel pour l'écoute citoyenne au Sénégal :
> services publics (SENELEC, SEN_EAU, TER) & Fintech (WAVE, Orange Money, Free Money).
> Encadrant : *Mr Ahmed Ben Sidy Bouya SEYE — Senior Big Data & AI Engineer, Groupe Sonatel*

---

## Table des matières

1. [Vision & Enjeux](#1-vision--enjeux)
2. [Architecture](#2-architecture)
3. [Stack Technologique](#3-stack-technologique)
4. [Démarrage rapide](#4-démarrage-rapide)
5. [Structure du projet](#5-structure-du-projet)
6. [Pipelines](#6-pipelines)
7. [Privacy by Design](#7-privacy-by-design)
8. [Dashboards & Rapports](#8-dashboards--rapports)
9. [Tests](#9-tests)
10. [Soutenance](#10-soutenance)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Vision & Enjeux

Le Sénégal compte plus de **10 millions** d'utilisateurs de Mobile Money.
Une panne Wave ou Orange Money génère des milliers de plaintes en quelques minutes sur les réseaux sociaux.

**Vox-SN** construit un système de veille automatique capable de :

- collecter des posts citoyens en français, wolof et anglais
- détecter des **crises** et **fraudes** en temps réel
- produire des **analytics** sur la satisfaction des opérateurs
- générer des **rapports de crise** automatisés
- déclencher des **alertes** avant qu'une crise ne dégénère

---

## 2. Architecture

```
[ SOURCES ] ──► [ NiFi ] ──► [ KAFKA ] ──► [ SPARK NLP ] ──► [ STORAGE ]
 Posts/Fintech    Orchest.    social_raw    Nettoyage,        HBase / HDFS
 Simulateur       & Routage   social_anlz   Scoring,             │
 (FR/WO/EN)       par service               Classification       ▼
                                                 │           [ ANALYTICS ]
                                                 ▼            Apache Hive
                                          [ ALERT LAYER ]    vue_battle_mm
                                          Crises & Fraudes   vue_parts_voix
                                          (score < -0.5)     vue_alertes
```

Le pipeline complet : **NiFi → Kafka → Spark Streaming NLP → HBase/Hive → Airflow/MLflow**.

---

## 3. Stack Technologique

| Couche             | Technologie              | Rôle                                                       |
|--------------------|--------------------------|------------------------------------------------------------|
| Ingestion          | Apache NiFi 1.23         | Collecte multi-canaux, routage par opérateur               |
| Message Broker     | Apache Kafka 3.3         | Topics `social_raw`, `social_analyzed`, `social_sentiment_agg` |
| NLP Processing     | Spark NLP 5.1.4 + MLlib  | Tokenisation, stopwords FR/Wolof, TF-IDF, scoring          |
| Classification ML  | Spark MLlib              | Logistic Regression (sentiment) + Random Forest (catégorie) |
| Stockage rapide    | HBase 2.1                | Alertes temps réel, agrégats horaires                      |
| Stockage analytique| Apache Hive 3.1          | Vues Battle Mobile Money, parts de voix                    |
| Validation         | Pandera + regex PII      | Schéma strict + détection numéros sénégalais               |
| MLOps              | Airflow 2.7 + MLflow     | Recalcul horaire + réentraînement conditionnel             |
| Orchestration      | Docker Compose           | Tous les services en local                                 |

---

## 4. Démarrage rapide

### Prérequis

- **RAM** : 16 Go (14 Go disponibles)
- **CPU** : 4 cœurs
- **OS** : Ubuntu 22.04 LTS (ou WSL2)
- **Docker Engine** : 24.x
- **Java** : OpenJDK 11
- **Python** : 3.9+

### Installation

```bash
# 1. Cloner le projet
git clone https://github.com/<votre-user>/vox-sn.git
cd vox-sn

# 2. Lancer le setup automatique
chmod +x setup.sh
./setup.sh

# 3. Démarrer la stack
make up

# 4. Initialiser HBase (une seule fois)
make hbase-init

# 5. Initialiser Hive
make hive-init

# 6. Lancer le simulateur de posts
make produce

# 7. Lancer le pipeline NLP Spark
make stream
```

### Interfaces web

| Service     | URL                          | Identifiants |
|-------------|------------------------------|--------------|
| NiFi        | http://localhost:8081/nifi   | -            |
| Spark UI    | http://localhost:8080        | -            |
| HBase UI    | http://localhost:16010       | -            |
| Airflow     | http://localhost:8082        | admin/admin  |
| MLflow      | http://localhost:5000        | -            |

---

## 5. Structure du projet

```
vox-sn/
├── docker/                  # Images custom + Dockerfiles
├── dags/                    # DAGs Airflow
│   └── vox_sn_monitoring_dag.py
├── spark/                   # Pipelines Spark
│   ├── streaming_sentiment.py
│   ├── train_classifier.py
│   ├── schema.py
│   └── lexique_sn.py
├── kafka/                   # Producteurs / consommateurs Kafka
│   ├── kafka_producer_vox.py
│   └── kafka_consumer_check.py
├── hbase/                   # Initialisation HBase
│   └── hbase_setup.py
├── hive/                    # Scripts DDL Hive
│   └── hive_setup.sql
├── nifi/                    # Templates NiFi
│   └── templates/vox_sn_flow.xml
├── models/                  # Modèles ML persistants (HDFS-like)
├── notebooks/               # Jupyter notebooks d'analyse
│   └── 01_exploration_sentiment.ipynb
├── dashboards/              # Dashboards + rapports
│   ├── rapport_crise.py
│   └── dashboard_battle_mm.py
├── scripts/                 # Scripts utilitaires
│   ├── test_pipeline.sh
│   ├── test_hive.sh
│   └── inject_crisis.py
├── tests/                   # Tests pytest
│   ├── test_schema.py
│   ├── test_lexique.py
│   ├── test_kafka.py
│   └── test_nlp_udf.py
├── data/posts/              # Données simulées
├── docs/                    # Documentation
│   ├── PLAYBOOK.md
│   ├── ARCHITECTURE.md
│   ├── SOUTENANCE.md
│   └── diagrams/
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── setup.sh
├── Makefile
└── README.md
```

---

## 6. Pipelines

### 6.1 Pipeline d'ingestion (NiFi → Kafka)

- 6 routes RouteOnContent (WAVE, OM, FREE, SENELEC, SEN_EAU, TER)
- Sortie unifiée sur topic Kafka `social_raw`

### 6.2 Pipeline NLP (Spark Streaming)

1. Lecture Kafka `social_raw`
2. **Privacy Layer** : anonymisation SHA-256 + drop des PII
3. Normalisation (regex, accents préservés)
4. Suppression stopwords (FR + Wolof)
5. Scoring sentiment lexical (lexique Wolof/FR)
6. Catégorisation (TARIF/TECHNIQUE/FRAUDE/SERVICE_CLIENT)
7. Labelling (NEGATIF_FORT, NEGATIF, NEUTRE, POSITIF)
8. Statut d'alerte (CRISE si score < -0.5 + catégorie sensible)
9. Écriture vers `social_analyzed` + agrégats vers `social_sentiment_agg`

### 6.3 Pipeline ML

- **Modèle 1** : Logistic Regression sur TF-IDF → label sentiment
- **Modèle 2** : Random Forest sur TF-IDF → catégorie de plainte
- Métrique : F1 multi-classe
- Cible : F1 > 0.7

### 6.4 Pipeline d'orchestration (Airflow)

- DAG `vox_sn_monitoring` toutes les heures
- T1 : `recalculate_sentiment` (Hive `INSERT OVERWRITE`)
- T2 : `detect_crises` (BranchPythonOperator)
- T3 : `trigger_retrain` (si > 3 opérateurs en CRISE)

---

## 7. Privacy by Design

> **Règle d'or** : aucun `user_id` ou `phone_number` brut ne quitte la Privacy Layer.

| Étape           | Action                                                          |
|-----------------|-----------------------------------------------------------------|
| Détection PII   | Regex `+221xxxxxxxxx`, `7xxxxxxxx`, numéros transaction         |
| Anonymisation   | `SHA-256(user_id + SALT)` → `citizen_id_secure`                 |
| Suppression     | `drop('user_id', 'phone_number')` après hash                    |
| Rétention HBase | TTL 72h sur `vox:alertes`                                       |
| Logs            | Aucun PII brut dans les logs Spark, Hive, Airflow               |

Toute fuite de PII = **0/4** sur la note Privacy. Cf. `docs/PRIVACY.md`.

---

## 8. Dashboards & Rapports

### Battle Mobile Money

```sql
SELECT * FROM vox_sn.vue_battle_mobile_money;
```

Comparaison Wave / Orange Money / Free Money sur 7 jours glissants :
sentiment moyen, % critique, nombre de fraudes, nombre de pannes.

### Rapport de Crise

```bash
python dashboards/rapport_crise.py
```

Génère `rapport_crise_wave.png` avec 4 panneaux :
- Timeline sentiment Wave
- Distribution des catégories de plaintes
- Comparaison Battle Mobile Money
- Wordcloud des plaintes (Wolof + FR)

---

## 9. Tests

```bash
# Tous les tests
make test

# Catégorie par catégorie
pytest tests/test_schema.py -v       # Pandera + détection PII
pytest tests/test_lexique.py -v      # Lexique Wolof/FR
pytest tests/test_nlp_udf.py -v      # UDFs de scoring
pytest tests/test_kafka.py -v        # Producteur Kafka
```

---

## 10. Soutenance

Voir `docs/SOUTENANCE.md` pour :
- scénario de démo (panne Wave simulée)
- storytelling Q&A
- captures attendues
- checklist 24h avant

---

## 11. Troubleshooting

| Symptôme                             | Solution                                                   |
|--------------------------------------|------------------------------------------------------------|
| `Connection refused kafka:9092`      | `docker compose restart kafka` puis attendre 30s           |
| `No module named pyspark`            | `source venv_vox/bin/activate`                             |
| HBase Thrift port 9090 ne répond pas | `docker exec hbase /entrypoint.sh start thrift`            |
| Spark OOM                            | Réduire `spark.sql.shuffle.partitions` à 2                 |
| Airflow DAG non visible              | `docker exec airflow airflow dags reserialize`             |
| NiFi UI 502                          | Attendre 60s (boot lent), puis F5                          |

Voir `docs/TROUBLESHOOTING.md` pour la liste complète.

---

## Licence & Contact

**Projet académique** — UADB Master 2 Big Data & IA — Année 2025-2026.

Encadrant : *Mr Ahmed Ben Sidy Bouya SEYE*, Senior Big Data & AI Engineer, Groupe Sonatel.

> *"Always be a solution, never a problem."*
