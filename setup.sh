#!/usr/bin/env bash
# =============================================================================
# Vox-SN — setup.sh
# Bootstrap complet de l'environnement (Docker + Python + topics Kafka + HBase)
# Usage : ./setup.sh
# =============================================================================

set -euo pipefail

# --- Couleurs pour les logs lisibles ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*"; }

# =============================================================================
# 1. Vérification environnement
# =============================================================================
log_info "==========================================="
log_info " Vox-SN — Bootstrap de l'environnement"
log_info "==========================================="

log_info "Vérification Docker..."
if ! command -v docker &> /dev/null; then
    log_error "Docker n'est pas installé. Installer Docker Engine 24.x minimum."
    exit 1
fi
DOCKER_VERSION=$(docker --version)
log_ok "$DOCKER_VERSION"

log_info "Vérification Docker Compose..."
if ! docker compose version &> /dev/null; then
    log_error "Docker Compose v2 requis. Mettre à jour Docker."
    exit 1
fi
log_ok "$(docker compose version)"

log_info "Vérification Java..."
if ! command -v java &> /dev/null; then
    log_warn "Java non détecté. Spark NLP requiert Java 11."
    log_warn "  sudo apt install openjdk-11-jdk"
else
    JAVA_VERSION=$(java -version 2>&1 | head -n1)
    log_ok "$JAVA_VERSION"
fi

log_info "Vérification Python..."
if ! command -v python3 &> /dev/null; then
    log_error "Python 3 n'est pas installé."
    exit 1
fi
PY_VERSION=$(python3 --version)
log_ok "$PY_VERSION"

log_info "Vérification RAM..."
if command -v free &> /dev/null; then
    # Linux
    RAM_AVAIL_GB=$(free -g | awk '/^Mem:/{print $7}')
elif command -v vm_stat &> /dev/null; then
    # macOS : parse vm_stat via Python pour éviter les subtilités awk
    RAM_AVAIL_GB=$(python3 - <<'PYEOF'
import subprocess, re
out = subprocess.check_output(['vm_stat'], text=True)
page = int(re.search(r'page size of (\d+)', out).group(1))
free     = int(re.search(r'Pages free:\s+(\d+)', out).group(1))
inactive = int(re.search(r'Pages inactive:\s+(\d+)', out).group(1))
print(int((free + inactive) * page / 1073741824))
PYEOF
)
else
    log_warn "Impossible de vérifier la RAM."
    RAM_AVAIL_GB=99
fi
if [ "$RAM_AVAIL_GB" -lt 8 ]; then
    log_warn "Seulement ${RAM_AVAIL_GB}Go de RAM disponibles. Minimum recommandé : 14 Go."
else
    log_ok "${RAM_AVAIL_GB}Go de RAM disponibles."
fi

# =============================================================================
# 2. Création des dossiers nécessaires
# =============================================================================
log_info "Création de l'arborescence de données..."
mkdir -p data/posts
mkdir -p models
mkdir -p airflow/logs
mkdir -p airflow/plugins
mkdir -p nifi/templates
chmod -R 777 airflow/logs 2>/dev/null || true
log_ok "Dossiers créés."

# =============================================================================
# 3. Configuration .env
# =============================================================================
if [ ! -f .env ]; then
    log_info "Création de .env à partir de .env.example..."
    cp .env.example .env
    log_ok ".env créé. Pensez à modifier CITIZEN_SECRET_SALT en production."
else
    log_ok ".env déjà présent."
fi

# =============================================================================
# 4. Environnement Python virtuel
# =============================================================================
if [ ! -d venv_vox ]; then
    log_info "Création du virtualenv Python..."
    python3 -m venv venv_vox
    log_ok "venv_vox créé."
fi

log_info "Activation venv et installation des dépendances..."
# shellcheck source=/dev/null
source venv_vox/bin/activate
pip install --upgrade pip --quiet

# Sur macOS, google-re2 (dep transitive d'Airflow) nécessite :
#  • les headers abseil/re2 (Homebrew)
#  • C++17 (abseil 2026+ rejette C++14)
#  • pybind11 (header-only, requis par le build C++ de google-re2)
if [[ "$OSTYPE" == "darwin"* ]]; then
    if command -v brew &> /dev/null; then
        log_info "macOS — installation de re2 + abseil via Homebrew..."
        HOMEBREW_NO_AUTO_UPDATE=1 brew install re2 abseil --quiet 2>/dev/null || \
            log_warn "brew install re2/abseil échoué."
    fi

    log_info "macOS — pré-installation de pybind11 + flag C++17 pour google-re2..."
    pip install "pybind11>=2.11" --quiet

    PYBIND11_INC=$(python3 -c "import pybind11; print(pybind11.get_include())" 2>/dev/null || echo "")
    BREW_PREFIX=$(brew --prefix 2>/dev/null || echo "/usr/local")
    export CXXFLAGS="-std=c++17 -I${BREW_PREFIX}/include ${PYBIND11_INC:+-I${PYBIND11_INC}}"
    export CFLAGS="-std=c++17"
    export LDFLAGS="-L${BREW_PREFIX}/lib"

    log_ok "Environnement C++ configuré (C++17, abseil=${BREW_PREFIX}/include, pybind11)."
fi

pip install -r requirements.txt --quiet
log_ok "Dépendances Python installées."

# =============================================================================
# 5. Démarrage de l'infrastructure
# =============================================================================
log_info "Démarrage Zookeeper..."
docker compose up -d zookeeper
sleep 10

log_info "Démarrage Kafka, NiFi, HBase, Hive Metastore..."
docker compose up -d kafka nifi hbase hive-metastore
sleep 30

log_info "Démarrage Spark master + worker, Airflow, MLflow..."
docker compose up -d spark-master spark-worker airflow mlflow
sleep 15

log_info "Vérification des conteneurs..."
docker compose ps

# =============================================================================
# 6. Création des topics Kafka
# =============================================================================
log_info "Création des topics Kafka..."
docker exec vox_kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic social_raw \
    --partitions 3 --replication-factor 1 || true
docker exec vox_kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic social_analyzed \
    --partitions 3 --replication-factor 1 || true
docker exec vox_kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic social_sentiment_agg \
    --partitions 3 --replication-factor 1 || true
log_ok "Topics Kafka créés."

# =============================================================================
# 7. Initialisation HBase
# =============================================================================
log_info "Initialisation des tables HBase..."
python3 hbase/hbase_setup.py || log_warn "Init HBase à relancer manuellement."

# =============================================================================
# 8. Récapitulatif final
# =============================================================================
echo ""
log_info "==========================================="
log_ok   " Setup terminé avec succès !"
log_info "==========================================="
echo ""
echo "Interfaces web disponibles :"
echo "  • NiFi     : http://localhost:8081/nifi"
echo "  • Spark    : http://localhost:8080"
echo "  • HBase    : http://localhost:16010"
echo "  • Airflow  : http://localhost:8082    (admin/admin)"
echo "  • MLflow   : http://localhost:5001"
echo ""
echo "Prochaines étapes :"
echo "  make produce        # Lancer le simulateur de posts"
echo "  make stream         # Lancer le pipeline NLP Spark"
echo "  make hive-init      # Initialiser les tables Hive"
echo "  make test           # Lancer la suite de tests"
echo ""
