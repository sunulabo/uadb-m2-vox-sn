# Vox-SN — Analyse de Sentiment Services Publics & Fintech
**UADB | Master 2 Big Data & IA | 2025-2026**  
**Enseignant : Mr Ahmed Ben Sidy Bouya SEYE — Senior Big Data & AI Engineer | Groupe Sonatel**

---

##  Description
Vox-SN est un système de veille automatique qui collecte et analyse les posts citoyens
concernant les services publics (SENELEC, SEN_EAU, TER) et les solutions de Mobile Money
(Wave, Orange Money, Free Money) au Sénégal.

##  Architecture

##  Stack Technologique
| Couche | Technologie | Rôle |
|--------|-------------|------|
| Ingestion | Apache NiFi 1.23 | Collecte multi-canaux |
| Message Broker | Apache Kafka 3.3 | Topics social_raw, social_analyzed |
| NLP Processing | Spark NLP + MLlib | Sentiment, classification |
| Stockage | HBase 2.1 | Alertes temps réel |
| Analytics | Apache Hive 3.1 | Vues Battle Mobile Money |
| Validation | Pandera + regex | Schéma strict + détection PII |
| MLOps | Airflow 2.7 + MLflow | Recalcul horaire + réentraînement |

##  Prérequis
- RAM : 16 Go minimum
- CPU : 4 cœurs
- OS : Ubuntu 22.04 LTS
- Docker Engine 24.x
- Java 11 (requis par Spark NLP)
- Python 3.9

##  Installation & Démarrage

### 1. Cloner et configurer l'environnement
```bash
git clone <repo>
cd vox-sn
python3.9 -m venv venv_vox && source venv_vox/bin/activate
pip install -r requirements.txt
```

### 2. Démarrer l'infrastructure Docker
```bash
# Zookeeper d'abord
docker compose up -d zookeeper
sleep 15

# Kafka + HBase + NiFi + Hive
docker compose up -d kafka hbase nifi hive-metastore
sleep 30

# Vérifier que tout tourne
docker compose ps
```

### 3. Initialiser HBase (une seule fois)
```bash
# Créer le namespace vox
docker exec hbase bash -c "echo \"create_namespace 'vox'\" | hbase shell"

# Créer les tables
python hbase_setup.py
```

### 4. Initialiser Hive (une seule fois)
```bash
docker exec -u root hive-metastore mkdir -p /user/hive/warehouse
docker exec -u root hive-metastore chmod 777 /user/hive/warehouse
docker cp hive_setup.sql hive-metastore:/tmp/hive_setup.sql
docker exec hive-metastore beeline -u jdbc:hive2://localhost:10000 -f /tmp/hive_setup.sql
```

### 5. Lancer le pipeline (2 terminaux)
```bash
# Terminal 1 — Simulateur de posts citoyens
source venv_vox/bin/activate
python kafka_producer_vox.py

# Terminal 2 — Pipeline NLP Spark Streaming
source venv_vox/bin/activate
python streaming_sentiment.py
```

### 6. Entraîner les modèles ML
```bash
python train_classifier.py
# [Sentiment] F1 = 0.73 
# [Catégorie] F1 = 0.14
# Modèles sauvegardés dans models/classifier_latest/
```

### 7. Générer le rapport de crise
```bash
python rapport_crise.py
# Rapport généré : rapport_crise_wave.png
```

##  Interfaces Web
| Service | URL |
|---------|-----|
| NiFi | http://localhost:8081 |
| Spark | http://localhost:8080 |
| HBase | http://localhost:16010 |
| Airflow | http://localhost:8082 |

##  Structure du Projet

##  Privacy by Design
- **SHA-256** sur user_id → citizen_id_secure
- **Suppression** de phone_number avant stockage
- **Détection PII** par regex (numéros sénégalais +221xxxxxxxxx)
- **Aucun PII brut** dans HBase, Hive ou logs Spark

## Résultats Modèles ML
| Modèle | Algorithme | F1 Score |
|--------|-----------|---------|
| Sentiment | Logistic Regression + TF-IDF | 0.73 |
| Catégorie | Random Forest + TF-IDF | 0.14 |

##  Notes importantes
- Java 11 obligatoire (Spark NLP incompatible Java 17+)
- Kafka externe sur `localhost:9093` (interne `kafka:9092`)
- Namespace HBase `vox` doit être créé avant `hbase_setup.py`
- Les modèles s'améliorent avec les vraies données du pipeline
