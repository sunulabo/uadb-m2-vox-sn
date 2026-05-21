#!/bin/bash
# ==============================================================================
# scripts/smoke_test.sh — Test de bout en bout de Vox-SN
# ==============================================================================
# Vérifie en 2 minutes que tous les composants critiques fonctionnent.
# Usage : ./scripts/smoke_test.sh
# ==============================================================================

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

PASS=0
FAIL=0

check() {
  local nom="$1"
  local cmd="$2"
  printf "  %-50s" "$nom"
  if eval "$cmd" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ OK${NC}"
    PASS=$((PASS+1))
  else
    echo -e "${RED}✗ FAIL${NC}"
    FAIL=$((FAIL+1))
  fi
}

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         Vox-SN — Smoke Test (test de fumée)                ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo

# ------------------------------------------------------------------
# 1. Docker & containers
# ------------------------------------------------------------------
echo -e "${YELLOW}[1/6] Conteneurs Docker${NC}"
check "Docker daemon"                "docker info"
check "Zookeeper UP"                 "docker ps | grep -q vox_zookeeper"
check "Kafka UP"                     "docker ps | grep -q vox_kafka"
check "NiFi UP"                      "docker ps | grep -q vox_nifi"
check "Spark master UP"              "docker ps | grep -q vox_spark_master"
check "Spark worker UP"              "docker ps | grep -q vox_spark_worker"
check "HBase UP"                     "docker ps | grep -q vox_hbase"
check "Hive metastore UP"            "docker ps | grep -q vox_hive_metastore"
check "Hive server UP"               "docker ps | grep -q vox_hive_server"
check "Airflow UP"                   "docker ps | grep -q vox_airflow"
check "MLflow UP"                    "docker ps | grep -q vox_mlflow"
echo

# ------------------------------------------------------------------
# 2. Endpoints HTTP
# ------------------------------------------------------------------
echo -e "${YELLOW}[2/6] Interfaces Web${NC}"
check "Spark UI (8080)"              "curl -fsS http://localhost:8080 -o /dev/null"
check "NiFi UI (8081)"               "curl -fsS -k https://localhost:8081 -o /dev/null || curl -fsS http://localhost:8081 -o /dev/null"
check "Airflow UI (8082)"            "curl -fsS http://localhost:8082/health -o /dev/null"
check "HBase UI (16010)"             "curl -fsS http://localhost:16010 -o /dev/null"
check "Kafka UI (8090)"              "curl -fsS http://localhost:8090 -o /dev/null"
check "MLflow UI (5000)"             "curl -fsS http://localhost:5000 -o /dev/null"
echo

# ------------------------------------------------------------------
# 3. Kafka topics
# ------------------------------------------------------------------
echo -e "${YELLOW}[3/6] Topics Kafka${NC}"
check "Topic social_raw existe"      "docker exec vox_kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q social_raw"
check "Topic social_analyzed existe" "docker exec vox_kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q social_analyzed"
check "Topic social_sentiment_agg"   "docker exec vox_kafka kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q social_sentiment_agg"
echo

# ------------------------------------------------------------------
# 4. HBase
# ------------------------------------------------------------------
echo -e "${YELLOW}[4/6] HBase${NC}"
check "Connexion Thrift (9090)"      "nc -z localhost 9090"
check "Table vox:posts"              "echo 'list' | docker exec -i vox_hbase hbase shell 2>&1 | grep -q 'vox:posts'"
check "Table vox:alertes"            "echo 'list' | docker exec -i vox_hbase hbase shell 2>&1 | grep -q 'vox:alertes'"
echo

# ------------------------------------------------------------------
# 5. Hive
# ------------------------------------------------------------------
echo -e "${YELLOW}[5/6] Hive${NC}"
check "Beeline (10000)"              "nc -z localhost 10000"
check "Base vox_sn existe"           "docker exec vox_hive_server beeline -u jdbc:hive2://localhost:10000 -e 'SHOW DATABASES' 2>&1 | grep -q vox_sn"
echo

# ------------------------------------------------------------------
# 6. Code Python local
# ------------------------------------------------------------------
echo -e "${YELLOW}[6/6] Code Python${NC}"
check "Import lexique_sn"            "python3 -c 'import sys; sys.path.insert(0, \"spark\"); import lexique_sn'"
check "Import schema (Pandera)"      "python3 -c 'import sys; sys.path.insert(0, \"spark\"); import schema'"
check "Tests pytest disponibles"     "python3 -m pytest --collect-only tests/ 2>&1 | grep -q 'test'"
echo

# ------------------------------------------------------------------
# Récapitulatif
# ------------------------------------------------------------------
TOTAL=$((PASS+FAIL))
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "  Total : ${PASS}/${TOTAL} OK"
if [ $FAIL -eq 0 ]; then
  echo -e "  ${GREEN}✓ Tout est OK ! Le projet est prêt pour la démo.${NC}"
  exit 0
else
  echo -e "  ${RED}✗ ${FAIL} test(s) en échec — consulter PLAYBOOK.md (troubleshooting)${NC}"
  exit 1
fi
