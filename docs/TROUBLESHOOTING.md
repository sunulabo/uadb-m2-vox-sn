# Troubleshooting — Vox-SN

Guide de résolution rapide des incidents courants. **À garder ouvert pendant la démo.**

---

## Diagnostic général en 3 commandes

```bash
make ps                          # tous les services UP ?
docker-compose logs --tail=50    # erreurs récentes ?
./scripts/smoke_test.sh          # test de bout-en-bout
```

---

## 1. Kafka

### Symptôme : `Connection to node -1 (localhost/127.0.0.1:9092) could not be established`
**Causes possibles** :
- Kafka pas encore prêt (attendre 30s après `make up`)
- Port 9092 occupé par un autre processus

**Résolution** :
```bash
docker-compose ps kafka                          # status = "healthy" ?
docker-compose logs kafka | tail -50             # erreurs ?
lsof -i :9092                                    # port libre ?
docker-compose restart kafka
```

### Symptôme : Topic `social_raw` n'existe pas
```bash
docker exec -it vox_kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic social_raw \
  --partitions 6 --replication-factor 1
```

### Symptôme : Producer envoie mais consumer ne reçoit rien
- Vérifier le `group.id` : chaque consumer doit avoir un ID unique sinon ils se partagent les partitions
- `--from-beginning` pour relire depuis offset 0

---

## 2. NiFi

### Symptôme : Interface inaccessible sur `http://localhost:8081`
**NiFi met 60-90s à démarrer**. Attendre.

```bash
docker-compose logs -f nifi | grep "NiFi has started"
```

### Symptôme : `Invalid username/password`
Identifiants par défaut :
- User : `admin`
- Pass : `voxsnadminpwd2025` (16 caractères, sensible casse)

Si oubli :
```bash
docker exec -it vox_nifi /opt/nifi/nifi-current/bin/nifi.sh set-single-user-credentials admin newPassword12345
docker-compose restart nifi
```

### Symptôme : Le processeur `PublishKafka` est en erreur (icône rouge)
- Vérifier que `Kafka Brokers` = `kafka:9092` (pas `localhost`)
- Le réseau Docker `vox_sn_net` doit être partagé
- Tester depuis NiFi : `docker exec -it vox_nifi ping kafka`

---

## 3. Spark Streaming

### Symptôme : `java.lang.OutOfMemoryError: Java heap space`
**Cause** : driver ou executor sous-dimensionné.

```bash
# Augmenter dans docker-compose.yml :
SPARK_DRIVER_MEMORY: 2g
SPARK_EXECUTOR_MEMORY: 2g

docker-compose up -d spark-master spark-worker
```

### Symptôme : `ClassNotFoundException: KafkaSource`
Le package Kafka n'est pas chargé. Lancer le job avec :
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  spark/streaming_sentiment.py
```

### Symptôme : Spark NLP `Could not load model`
```bash
# Pré-télécharger le modèle :
docker exec -it vox_spark python -c "
from sparknlp.pretrained import PretrainedPipeline
PretrainedPipeline('explain_document_dl', lang='fr')
"
```

### Symptôme : Streaming traite mais HBase reste vide
- Vérifier le batch interval : `trigger(processingTime='10 seconds')`
- Logs du `foreachBatch` : doit afficher "Batch X : Y rows written"
- Tester HBase :
```bash
docker exec -it vox_hbase hbase shell
> scan 'vox:posts', {LIMIT => 5}
```

---

## 4. HBase

### Symptôme : `org.apache.hadoop.hbase.MasterNotRunningException`
HBase met **2 à 3 minutes** à démarrer en mode standalone.

```bash
docker-compose logs -f hbase | grep "Master became active"
```

### Symptôme : Connexion Thrift refusée (port 9090)
```bash
# Activer Thrift manuellement :
docker exec -it vox_hbase hbase thrift start -p 9090 &
```

### Symptôme : `Table vox:posts not found`
```bash
docker exec -it vox_hbase hbase shell -f /scripts/hbase_setup.txt
# ou
python hbase/hbase_setup.py
```

---

## 5. Hive

### Symptôme : `beeline: Connection refused`
```bash
docker-compose ps hive-server     # status ?
docker-compose logs hive-server | tail -50
```

### Symptôme : Metastore inaccessible
```bash
docker-compose restart hive-metastore
sleep 30
docker-compose restart hive-server
```

### Symptôme : `Database vox_sn does not exist`
```bash
docker exec -it vox_hive_server beeline -u jdbc:hive2://localhost:10000 \
  -f /scripts/hive_setup.sql
```

---

## 6. Airflow

### Symptôme : DAG en `failed` au premier run
Probablement un import error. Vérifier :
```bash
docker exec -it vox_airflow airflow dags list-import-errors
```

### Symptôme : Webserver UI inaccessible (port 8082)
```bash
docker-compose logs airflow | grep "Listening at"
# Doit afficher : Listening at: http://0.0.0.0:8080
```
Note : Airflow écoute sur 8080 dans le conteneur, mais le compose le mappe à 8082 sur l'hôte.

### Symptôme : Tasks bloquées en `queued`
- Vérifier l'exécuteur : doit être `LocalExecutor` ou `CeleryExecutor`
- Compter les slots : `airflow pools list`

---

## 7. MLflow

### Symptôme : UI vide, aucune expérience
- L'URL de tracking dans le script doit pointer vers `http://mlflow:5000`
- Tester depuis Python :
```python
import mlflow
mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("vox_sn_sentiment")
with mlflow.start_run():
    mlflow.log_param("test", 1)
```

---

## 8. Docker général

### Symptôme : `Cannot connect to the Docker daemon`
```bash
sudo systemctl start docker          # Linux
# OU lancer Docker Desktop manuellement (Mac/Windows)
```

### Symptôme : `port is already allocated`
```bash
lsof -i :8080                        # quel proc ?
kill -9 <PID>                        # OU changer le port dans docker-compose.yml
```

### Symptôme : Disque plein
```bash
docker system prune -a --volumes     # ATTENTION : supprime tout ce qui est non utilisé
df -h
```

### Reset complet (nuke option)
```bash
make down
docker volume prune -f
docker network prune -f
docker system prune -a -f
make up
```

---

## 9. Pendant la démo : plan B

| Plante | Action immédiate |
|---|---|
| Dashboard ne se charge pas | Ouvrir le screenshot dans `demo_backup/` |
| Kafka muet | `docker-compose restart kafka` (30s) puis continuer sur slide |
| NiFi rouge | Passer à Spark, dire « le flux NiFi est représenté ici dans le diagramme » |
| `inject_crisis` échoue | Avoir une fenêtre HBase déjà ouverte avec données pré-injectées |
| Tout est mort | Vidéo backup de 3 min pré-enregistrée |

---

## 10. Logs utiles à connaître

```bash
# Tout en une fois :
docker-compose logs -f --tail=50

# Service spécifique :
docker-compose logs -f kafka spark-master

# Filtrer les erreurs :
docker-compose logs | grep -iE 'error|exception|failed'

# Vider les logs (économiser disque) :
docker-compose down
rm -rf airflow/logs/*
make up
```

---

## Contacts utiles

- Encadrant : Mr Ahmed Ben Sidy Bouya SEYE — Sonatel
- Documentation officielle :
  - Spark : <https://spark.apache.org/docs/3.3.2/structured-streaming-programming-guide.html>
  - Kafka : <https://kafka.apache.org/documentation/>
  - HBase : <https://hbase.apache.org/book.html>
  - Airflow : <https://airflow.apache.org/docs/>
