# =============================================================================
# Vox-SN — Makefile
# Cibles principales pour le pilotage du projet
# Usage : make help
# =============================================================================

.PHONY: help setup up down restart logs ps clean \
        produce stream consume \
        hbase-init hive-init kafka-topics airflow-init \
        train report dashboard \
        test test-schema test-lexique test-udf test-kafka \
        format lint zip

# Variables
SHELL        := /bin/bash
COMPOSE      := docker compose
VENV         := venv_vox
PYTHON       := $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,python3)
SPARK_SUBMIT := docker exec spark-master spark-submit --master spark://spark-master:7077

# Couleurs
GREEN := \033[0;32m
BLUE  := \033[0;34m
NC    := \033[0m

help: ## Affiche cette aide
	@echo -e "$(BLUE)╔══════════════════════════════════════════════════════════╗$(NC)"
	@echo -e "$(BLUE)║         Vox-SN — Pilotage projet (Makefile)              ║$(NC)"
	@echo -e "$(BLUE)╚══════════════════════════════════════════════════════════╝$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# -----------------------------------------------------------------------------
# Cycle de vie infrastructure
# -----------------------------------------------------------------------------
setup: ## Bootstrap initial (1ère fois)
	@chmod +x setup.sh && ./setup.sh

up: ## Démarre tous les services
	$(COMPOSE) up -d
	@echo -e "$(GREEN)Services démarrés. Attendre 30s avant de produire.$(NC)"

down: ## Arrête et supprime tous les services
	$(COMPOSE) down

restart: down up ## Redémarre tout proprement

logs: ## Affiche les logs (10 dernières lignes par conteneur)
	$(COMPOSE) logs --tail=10 -f

logs-spark: ## Logs Spark master
	$(COMPOSE) logs -f spark-master

logs-kafka: ## Logs Kafka
	$(COMPOSE) logs -f kafka

ps: ## Liste les conteneurs actifs
	$(COMPOSE) ps

clean: ## Nettoyage complet (volumes inclus, ATTENTION destructif)
	$(COMPOSE) down -v --remove-orphans
	rm -rf data/posts/*.json models/* airflow/logs/*

# -----------------------------------------------------------------------------
# Initialisation des composants
# -----------------------------------------------------------------------------
hbase-init: ## Crée les tables HBase (vox:posts, vox:alertes, vox:sentiment_agg)
	@test -f $(VENV)/bin/python || (echo "Erreur: venv absent — lancez ./setup.sh" && exit 1)
	@echo "Démarrage du serveur Thrift HBase (port 9090)..."
	@docker exec -d vox_hbase bash -c 'hbase thrift start -p 9090' && sleep 8
	@HBASE_HOST=localhost $(PYTHON) hbase/hbase_setup.py || \
		( echo "Fallback via réseau Docker (vox_hbase)..." && \
		  docker run --rm --network vox_sn_net \
		    -v "$(CURDIR):/app" -w /app \
		    -e HBASE_HOST=vox_hbase \
		    python:3.9-slim \
		    bash -c "pip install -q happybase thrift && python hbase/hbase_setup.py" )

hive-init: ## Crée les tables et vues Hive
	$(COMPOSE) up -d hive-server
	@echo "Attente du démarrage HiveServer2 (60s)..."
	@sleep 60
	docker cp hive/hive_setup.sql vox_hive_server:/tmp/hive_setup.sql
	@HIVE_CID=$$(docker inspect -f '{{.Id}}' vox_hive_server | cut -c1-12); \
	docker exec vox_hive_server beeline -u "jdbc:hive2://$$HIVE_CID:10000/" \
		-f /tmp/hive_setup.sql

airflow-init: ## Active le DAG Airflow vox_sn_monitoring
	docker exec vox_airflow airflow dags reserialize
	docker exec vox_airflow airflow dags unpause vox_sn_monitoring

kafka-topics: ## Crée les topics Kafka (social_raw, social_analyzed, social_sentiment_agg)
	@for topic in social_raw social_analyzed social_sentiment_agg; do \
		docker exec vox_kafka kafka-topics --bootstrap-server localhost:9092 \
			--create --if-not-exists --topic $$topic \
			--partitions 3 --replication-factor 1; \
	done
	@echo -e "$(GREEN)Topics Kafka créés.$(NC)"

# -----------------------------------------------------------------------------
# Pipeline runtime
# -----------------------------------------------------------------------------
produce: ## Lance le simulateur de posts citoyens (Kafka producer)
	@echo -e "$(GREEN)Démarrage simulateur Vox-SN (Ctrl+C pour stopper)$(NC)"
	KAFKA_BROKERS=localhost:9093 $(PYTHON) kafka/kafka_producer_vox.py

stream: ## Lance le pipeline NLP Spark Streaming
	@echo -e "$(GREEN)Lancement Spark Streaming NLP...$(NC)"
	$(SPARK_SUBMIT) \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
		--conf spark.jars.ivy=/tmp/.ivy \
		/opt/vox/spark/streaming_sentiment.py

consume: ## Visualise les posts analysés sur Kafka
	docker exec kafka kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 \
		--topic social_analyzed --from-beginning --max-messages 10

inject-crisis: ## Injecte 50 posts négatifs WAVE pour déclencher l'alerte CRISE
	$(PYTHON) scripts/inject_crisis.py

# -----------------------------------------------------------------------------
# Machine Learning
# -----------------------------------------------------------------------------
train: ## Entraîne les modèles Logistic Regression + Random Forest
	$(SPARK_SUBMIT) /opt/vox/spark/train_classifier.py \
		--output-path /opt/models/classifier_latest \
		--window-days 30

report: ## Génère le rapport de crise PNG
	$(PYTHON) dashboards/rapport_crise.py

dashboard: ## Lance le dashboard Battle Mobile Money (Plotly)
	$(PYTHON) dashboards/dashboard_battle_mm.py

# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
test: ## Lance toute la suite de tests pytest
	pytest tests/ -v --cov=spark --cov=kafka --cov-report=term-missing

test-schema: ## Tests Pandera + détection PII
	pytest tests/test_schema.py -v

test-lexique: ## Tests du lexique Wolof/FR
	pytest tests/test_lexique.py -v

test-udf: ## Tests des UDFs NLP
	pytest tests/test_nlp_udf.py -v

test-kafka: ## Tests producteur Kafka
	pytest tests/test_kafka.py -v

# -----------------------------------------------------------------------------
# Qualité de code
# -----------------------------------------------------------------------------
format: ## Formate le code Python (black)
	black spark/ kafka/ hbase/ dags/ dashboards/ tests/ scripts/

lint: ## Lint Python (flake8)
	flake8 spark/ kafka/ hbase/ dags/ dashboards/ tests/ scripts/ \
		--max-line-length=100 --exclude=venv_vox

# -----------------------------------------------------------------------------
# Packaging
# -----------------------------------------------------------------------------
zip: ## Crée vox-sn.zip prêt à livrer
	@echo -e "$(GREEN)Création archive vox-sn.zip$(NC)"
	cd .. && zip -r vox-sn.zip vox-sn \
		-x "vox-sn/venv_vox/*" \
		-x "vox-sn/.git/*" \
		-x "vox-sn/data/posts/*.json" \
		-x "vox-sn/__pycache__/*" \
		-x "vox-sn/*/__pycache__/*" \
		-x "vox-sn/airflow/logs/*" \
		-x "vox-sn/models/*"
	@echo -e "$(GREEN)Archive prête : ../vox-sn.zip$(NC)"

# Cible par défaut
.DEFAULT_GOAL := help
