# 📘 PLAYBOOK Vox-SN — Guide d'Exécution Complet

> Guide étape par étape, du `git clone` jusqu'à la soutenance.
> Aucune étape sautée, toutes les commandes exactes, tous les pièges documentés.

---

## 🗓️ Roadmap recommandée sur 4 semaines

| Semaine | Objectifs | Livrables |
|---------|-----------|-----------|
| **S1** | Setup environnement + Infrastructure Docker + tests Kafka | `docker-compose.yml` opérationnel, topics créés |
| **S2** | Pipeline NLP + Privacy + Lexique Wolof | `streaming_sentiment.py` qui scor et écrit |
| **S3** | Hive + Dashboards + ML + DAG Airflow | Battle Mobile Money + DAG horaire actif |
| **S4** | Tests + Rapport crise + Soutenance | Démo crise live + slides + ZIP final |

---

# PHASE 1 — Préparation environnement (Jour 1)

## 1.1 Vérifier les prérequis matériel

```bash
# RAM (minimum 14 Go libres)
free -h

# Espace disque (minimum 30 Go libres)
df -h /

# CPU (minimum 4 cœurs)
nproc
```

**Pièges fréquents :**
- ❌ < 14 Go RAM → Spark + HBase + Hive plantent simultanément
- ❌ WSL2 sous Windows : penser à augmenter la RAM dans `~/.wslconfig`

## 1.2 Installer les outils

### Docker + Docker Compose v2

```bash
# Ubuntu 22.04
sudo apt update
sudo apt install -y ca-certificates curl gnupg lsb-release
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Ajouter votre user au groupe docker
sudo usermod -aG docker $USER
newgrp docker

# Vérifier
docker --version          # Attendu : Docker version 24.x+
docker compose version    # Attendu : v2.x
```

### Java 11 (requis par Spark NLP)

```bash
sudo apt install -y openjdk-11-jdk
java -version   # Attendu : openjdk version "11.0.x"

# Définir JAVA_HOME (ajouter à ~/.bashrc)
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

### Python 3.9 + venv

```bash
sudo apt install -y python3.9 python3.9-venv python3-pip
python3.9 --version   # Attendu : Python 3.9.x
```

### Git + outils

```bash
sudo apt install -y git make jq
git config --global user.name "Votre Nom"
git config --global user.email "vous@uadb.edu.sn"
```

## 1.3 Cloner le projet

```bash
git clone <URL_REPO_GIT> vox-sn
cd vox-sn
ls -la   # Vérifier que tous les fichiers sont présents
```

---

# PHASE 2 — Setup projet (Jour 1)

## 2.1 Configuration des variables d'environnement

```bash
# Copier le template
cp .env.example .env

# Éditer (laisser les valeurs par défaut pour le dev local)
nano .env
```

Variables critiques :
```env
CITIZEN_SECRET_SALT=UADB_VOX_2025
AIRFLOW_USER=admin
AIRFLOW_PWD=admin
KAFKA_BROKERS=kafka:9092
```

⚠️ **En production** : régénérer `CITIZEN_SECRET_SALT` avec :
```bash
openssl rand -hex 32
```

## 2.2 Installation des dépendances Python

```bash
# Créer l'environnement virtuel
python3.9 -m venv venv_vox
source venv_vox/bin/activate

# Mettre pip à jour
pip install --upgrade pip wheel setuptools

# Installer
pip install -r requirements.txt

# Vérifier
pip list | grep -E "pyspark|kafka-python|pandera|airflow|mlflow"
```

**Erreur possible** : `pyhive` échoue → installer manuellement :
```bash
sudo apt install -y libsasl2-dev
pip install pyhive[hive] thrift_sasl
```

## 2.3 Créer les dossiers de travail

```bash
mkdir -p data/posts data/samples models/checkpoints airflow/logs airflow/plugins
chmod -R 777 airflow  # Airflow a besoin de droits d'écriture
```

## 2.4 Rendre les scripts exécutables

```bash
chmod +x setup.sh
chmod +x scripts/*.py
```

---

# PHASE 3 — Infrastructure Big Data (Jour 2)

## 3.1 Démarrer les services par paliers

> ⚠️ Ne pas faire `docker compose up -d` d'un coup. Démarrer par paliers évite les race conditions.

### Palier 1 : Zookeeper (10s)

```bash
docker compose up -d zookeeper
sleep 10
docker logs vox_zookeeper --tail 20
# Cherche "binding to port 0.0.0.0/0.0.0.0:2181"
```

### Palier 2 : Kafka + NiFi + HBase (30s)

```bash
docker compose up -d kafka kafka-ui nifi hbase
sleep 30

# Vérifier
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Attendu :
```
NAMES              STATUS
vox_kafka          Up 30 seconds
vox_kafka_ui       Up 30 seconds
vox_nifi           Up 30 seconds
vox_hbase          Up 30 seconds
vox_zookeeper      Up 40 seconds
```

### Palier 3 : Hive

```bash
docker compose up -d hive-metastore hive-server
sleep 45

# Vérifier que Hive Metastore est prêt
docker logs vox_hive_metastore --tail 30 2>&1 | grep -i "started"
```

### Palier 4 : Spark + Airflow + MLflow

```bash
docker compose up -d spark-master spark-worker airflow mlflow
sleep 20

# UI Web :
# - NiFi    : http://localhost:8081  (admin / voxsnadminpwd2025)
# - Spark   : http://localhost:8080
# - HBase   : http://localhost:16010
# - Airflow : http://localhost:8082  (admin / admin)
# - MLflow  : http://localhost:5000
# - Kafka UI: http://localhost:8090
```

## 3.2 Créer les topics Kafka

```bash
# Créer les 3 topics avec 3 partitions chacun
for topic in social_raw social_analyzed social_sentiment_agg; do
  docker exec vox_kafka kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic $topic \
    --partitions 3 \
    --replication-factor 1
done

# Lister pour vérifier
docker exec vox_kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

Attendu :
```
social_analyzed
social_raw
social_sentiment_agg
```

## 3.3 Initialiser HBase

```bash
# Depuis l'hôte (le script python doit pouvoir joindre HBase via le port mappé)
python hbase/hbase_setup.py
```

Attendu :
```
[INFO] Table vox:posts créée
[INFO] Table vox:alertes créée
[INFO] Table vox:sentiment_agg créée
```

**Pour vérifier dans HBase shell** :
```bash
docker exec -it vox_hbase hbase shell
# Dans le shell :
list
describe 'vox:posts'
quit
```

## 3.4 Initialiser Hive

```bash
# Copier le SQL dans le container puis l'exécuter
docker cp hive/hive_setup.sql vox_hive_server:/tmp/

docker exec vox_hive_server beeline \
  -u jdbc:hive2://localhost:10000 \
  -f /tmp/hive_setup.sql
```

Vérifier les tables :
```bash
docker exec vox_hive_server beeline \
  -u jdbc:hive2://localhost:10000 \
  -e "USE vox_sn; SHOW TABLES; SHOW VIEWS;"
```

---

# PHASE 4 — Pipeline NLP (Jour 3-4)

## 4.1 Démarrer le simulateur Kafka (terminal 1)

```bash
source venv_vox/bin/activate
python kafka/kafka_producer_vox.py
```

Attendu : 1 post toutes les 2 secondes affiché en console.

## 4.2 Vérifier le flux Kafka brut (terminal 2)

```bash
docker exec vox_kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic social_raw \
  --max-messages 5
```

Attendu : 5 posts JSON contenant `user_id` et `phone_number`.

## 4.3 Lancer le pipeline Spark NLP (terminal 3)

```bash
# Copier les modules dans le container Spark
docker cp spark/. vox_spark_master:/opt/spark-apps/

# Soumettre le job
docker exec vox_spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  --py-files /opt/spark-apps/lexique_sn.py,/opt/spark-apps/schema.py \
  /opt/spark-apps/streaming_sentiment.py
```

Attendu : logs Spark indiquant `Streaming query started`.

## 4.4 Vérifier le flux analysé (terminal 4)

```bash
docker exec vox_kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic social_analyzed \
  --max-messages 5 | jq
```

**Critères de validation Privacy** :
- ✅ `citizen_id_secure` présent (hash SHA-256)
- ❌ `user_id` ABSENT
- ❌ `phone_number` ABSENT
- ✅ `sentiment_score` présent
- ✅ `categorie` présent
- ✅ `statut_alerte` présent

---

# PHASE 5 — Privacy Layer (Jour 4)

## 5.1 Vérifier la détection PII en amont

```bash
# Lancer manuellement les tests Pandera
pytest tests/test_schema.py -v
```

Attendu : tous les tests `TestDetectionPII` passent.

## 5.2 Tester la suppression PII en aval

```bash
# Capturer 50 messages analysés
docker exec vox_kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic social_analyzed \
  --max-messages 50 \
  --timeout-ms 30000 > /tmp/analyzed.json

# Vérifier qu'aucun user_id n'a fuité
grep -c "user_id" /tmp/analyzed.json   # Attendu : 0
grep -c "phone_number" /tmp/analyzed.json # Attendu : 0
grep -c "citizen_id_secure" /tmp/analyzed.json # Attendu : > 0
```

⚠️ **Si user_id apparaît** → 0 point sur Privacy ! Vérifier `streaming_sentiment.py` ligne `.drop('user_id', 'phone_number')`.

---

# PHASE 6 — Machine Learning (Jour 5)

## 6.1 Préparer les données d'entraînement

Le pipeline streaming a déjà accumulé des données dans Kafka. Il faut maintenant les ingérer dans Hive via un consumer dédié OU charger un échantillon CSV.

```bash
# Option A : utiliser des données pré-générées
python scripts/seed_training_data.py

# Option B : exporter les 1000 derniers messages Kafka vers Hive
python kafka/kafka_consumer_check.py --to-hive --limit 1000
```

## 6.2 Lancer l'entraînement

```bash
docker exec vox_spark_master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/train_classifier.py \
  --output-path /opt/models/classifier_v1 \
  --window-days 30
```

Attendu :
```
Train=8245 | Test=2061
[Sentiment]  F1 = 0.812
[Catégorie]  F1 = 0.738
Modèles sauvegardés dans /opt/models/classifier_v1
```

## 6.3 Vérifier dans MLflow

Ouvrir http://localhost:5000 → onglet **Experiments** → `vox_sn_classifier`.

---

# PHASE 7 — Analytics & Dashboards (Jour 6)

## 7.1 Vérifier les vues Hive

```bash
docker exec vox_hive_server beeline \
  -u jdbc:hive2://localhost:10000 \
  -e "USE vox_sn; SELECT * FROM vue_battle_mobile_money;"
```

## 7.2 Générer le dashboard Battle Mobile Money

```bash
python dashboards/dashboard_battle_mm.py --output dashboards/battle_mm.png
```

## 7.3 Ouvrir le dashboard HTML

```bash
# Ouvrir dans le navigateur
xdg-open dashboards/index.html  # Linux
open dashboards/index.html      # macOS
```

---

# PHASE 8 — Airflow & Monitoring (Jour 7)

## 8.1 Activer les DAG

1. Ouvrir http://localhost:8082 (admin/admin)
2. Activer les DAG :
   - `vox_sn_monitoring` (toutes les heures)
   - `vox_sn_ingestion` (quotidien 02:00)
   - `vox_sn_retrain` (hebdomadaire dim 23:00)
3. Cliquer sur **▶ Trigger DAG** sur `vox_sn_monitoring` pour test manuel
4. Surveiller les logs : **Graph View → Task → Logs**

## 8.2 Provoquer une alerte CRISE de test

```bash
python scripts/inject_crisis.py --service WAVE --count 20
```

Effet attendu (dans les 60s) :
- 20 posts négatifs Wave dans Kafka
- Score moyen Wave descend sous -0.5
- Vue Hive `vue_alertes_crises` montre l'incident
- DAG Airflow détecte et logge "CRISE DÉTECTÉE : WAVE"

---

# PHASE 9 — Tests & Validation (Jour 8)

## 9.1 Suite pytest complète

```bash
# Tous les tests
pytest tests/ -v --tb=short

# Coverage
pip install pytest-cov
pytest tests/ --cov=spark --cov=kafka --cov-report=term-missing
```

Objectif : **≥ 80% de coverage** sur `spark/` et `kafka/`.

## 9.2 Tests d'intégration Kafka

```bash
# Test producer → consumer
python kafka/kafka_consumer_check.py --topic social_raw --duration 30
```

## 9.3 Tests Spark NLP

```bash
docker exec vox_spark_master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/test_nlp_unit.py
```

## 9.4 Validation Hive

```bash
# Compter les posts par service
docker exec vox_hive_server beeline -u jdbc:hive2://localhost:10000 -e "
USE vox_sn;
SELECT service_cible, COUNT(*) AS nb_posts
FROM posts_analyses
GROUP BY service_cible
ORDER BY nb_posts DESC;"
```

---

# PHASE 10 — Préparation soutenance (Jour 9-10)

## 10.1 Checklist pré-soutenance

### Veille de la soutenance
- [ ] `docker compose down && docker compose up -d` → vérifier que tout redémarre
- [ ] Lancer le simulateur pendant 1h pour accumuler ~1800 posts
- [ ] Tester `python scripts/inject_crisis.py` → vérifier que l'alerte remonte en < 60s
- [ ] Générer le rapport crise : `python dashboards/rapport_crise.py`
- [ ] Captures d'écran : NiFi (flux running), Spark UI (job actif), Airflow (DAG vert), HBase (tables), dashboard.

### Le matin de la soutenance
- [ ] Vérifier RAM dispo (`free -h` ≥ 12 Go)
- [ ] Démarrer le projet 15 min avant la soutenance
- [ ] Ouvrir les onglets dans l'ordre : `index.html` → NiFi → Spark UI → Airflow → MLflow → HBase UI
- [ ] Avoir un PowerPoint de secours en cas de panne
- [ ] Préparer une vidéo de démonstration de la crise (au cas où)

## 10.2 Scénario de démonstration (15 min)

| Min | Action | Quoi montrer |
|-----|--------|--------------|
| 0-2 | Slide intro | Problématique Wave/OM au Sénégal |
| 2-4 | Architecture (slide) | Schéma NiFi → Kafka → Spark → Hive |
| 4-6 | Démo NiFi | Flux running, routage 6 opérateurs |
| 6-9 | Démo Spark NLP | Logs streaming, message brut vs analyzed (montrer suppression PII) |
| 9-11 | **Démo crise live** | `python scripts/inject_crisis.py --service WAVE` → alerte HBase visible 30s après |
| 11-13 | Battle Mobile Money | Dashboard Plotly, comparaison Wave/OM/Free |
| 13-15 | Conclusion | Roadmap, perspectives, MLflow tracking |

## 10.3 Slides recommandés (PowerPoint)

1. **Garde** : Vox-SN, équipe, encadrant
2. **Problématique** : 10M utilisateurs Mobile Money, pannes Wave 2024
3. **Architecture** : diagramme global avec icônes
4. **Stack technique** : 8-10 logos
5. **Privacy by Design** : SHA-256, GDPR, Pandera
6. **Lexique Wolof** : exemples concrets ("dafa teye" = -0.9)
7. **ML & MLOps** : Logistic Regression F1=0.81, MLflow
8. **Dashboard Battle** : capture du dashboard HTML
9. **Alerte crise** : screenshot de la chute du sentiment Wave
10. **Démo live** : *"Place à la démonstration"*
11. **Conclusion** : ce qu'on a appris, limites, suite

## 10.4 Questions probables du jury

| Question | Réponse type |
|----------|--------------|
| *Pourquoi Spark Streaming et pas Flink ?* | Spark déjà choisi par l'écosystème Sonatel + intégration MLlib + DataFrame API plus accessible |
| *Comment le SHA-256 protège ?* | Hash irréversible + salt côté serveur ; même un dump HBase ne permet pas de retrouver l'identité |
| *Et si Spark plante ?* | Checkpoint Kafka → reprise au dernier offset commit |
| *Pourquoi un lexique et pas BERT ?* | Latence + pas de modèle BERT pré-entraîné Wolof à ce jour ; lexique extensible par les linguistes |
| *Comment vous monteriez en charge ?* | Augmenter partitions Kafka (12 au lieu de 3), 4 workers Spark, HBase région servers répartis |

---

# 🆘 Troubleshooting

## Kafka : "broker not available"

```bash
docker compose restart kafka
sleep 20
docker logs vox_kafka --tail 50 | grep -E "(ERROR|started)"
```

## HBase : "table already exists"

```bash
# Le script est idempotent : pas grave, c'est normal au 2e run
# Pour reset complet :
docker exec -it vox_hbase hbase shell <<EOF
disable_all 'vox:.*'
drop_all 'vox:.*'
exit
EOF
python hbase/hbase_setup.py
```

## Spark : "OutOfMemoryError"

```bash
# Augmenter la mémoire worker dans docker-compose.yml :
# SPARK_WORKER_MEMORY: 4G   (au lieu de 2G)
docker compose up -d spark-worker
```

## Airflow : "Broken DAG"

```bash
# Vérifier la syntaxe Python
python dags/vox_sn_monitoring_dag.py

# Rebuild la DB Airflow
docker exec vox_airflow airflow db reset -y
docker compose restart airflow
```

## Hive : "metastore not connected"

```bash
docker compose restart hive-metastore
sleep 30
docker compose restart hive-server
```

## Erreur générale "out of disk space"

```bash
# Nettoyer les volumes Docker
docker system prune -af --volumes
# ⚠️ Cela supprime TOUS les volumes : à utiliser uniquement en fin de projet
```

---

# 📦 Préparation du ZIP final

```bash
# Nettoyer avant zippage
rm -rf venv_vox
rm -rf airflow/logs/*
rm -rf models/checkpoints
rm -rf __pycache__ */__pycache__ */*/__pycache__

# Créer le ZIP
cd ..
zip -r vox-sn-projet-final.zip vox-sn/ \
  -x "vox-sn/venv_vox/*" \
  -x "vox-sn/airflow/logs/*" \
  -x "vox-sn/.git/*"

# Vérifier la taille (< 50 Mo)
ls -lh vox-sn-projet-final.zip
```

---

# 🎓 Stratégie pour la meilleure note

1. **Privacy = priorité absolue** : 4 pts perdus à la moindre fuite. Tester 3 fois.
2. **Démo crise live** : si elle marche, c'est l'effet "wahou". Si elle plante, **avoir la vidéo de secours**.
3. **Lexique Wolof riche** : ajouter 5-10 termes en plus pour montrer la valeur ajoutée locale.
4. **Tests pytest > 80%** : +0.5 pt bonus garanti.
5. **README impeccable** : avec captures d'écran de chaque service, README compte +0.5 pt.
6. **Storytelling** : raconter une histoire (panne Wave réelle au Sénégal en 2024) et montrer comment Vox-SN aurait alerté.
7. **Insister sur les techno** : Spark Streaming + Kafka + HBase + Hive + Airflow + MLflow = stack industrielle complète.
8. **Ne pas survendre** : reconnaître les limites (pas de vrai BERT Wolof, simulateur != réel) — le jury apprécie l'honnêteté technique.

---

*« Always be a solution, never a problem. » — Mr Ahmed Ben Sidy Bouya SEYE*

**Bonne chance pour la soutenance ! 🚀**
