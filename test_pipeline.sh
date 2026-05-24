#!/bin/bash
# test_pipeline.sh — Test end-to-end du pipeline Vox-SN
echo "======================================"
echo " TEST PIPELINE VOX-SN — END TO END"
echo "======================================"

# ── 1. Vérifier les topics Kafka ─────────────────────────────────────────
echo ""
echo "[1/5] Vérification des topics Kafka..."
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
echo "Attendu : social_raw, social_analyzed, social_sentiment_agg"

# ── 2. Démarrer le simulateur en arrière-plan ────────────────────────────
echo ""
echo "[2/5] Démarrage du simulateur de posts citoyens..."
source /home/abasse/vox-sn/venv_vox/bin/activate
python /home/abasse/vox-sn/kafka_producer_vox.py &
PRODUCER_PID=$!
echo "Simulateur démarré (PID=$PRODUCER_PID)"
sleep 5

# ── 3. Lire les posts bruts (avec PII) ───────────────────────────────────
echo ""
echo "[3/5] Lecture des posts bruts (topic social_raw — avec PII)..."
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic social_raw \
    --from-beginning \
    --max-messages 3 \
    --timeout-ms 10000

# ── 4. Lire les posts analysés (PII supprimés) ───────────────────────────
echo ""
echo "[4/5] Lecture des posts analysés (topic social_analyzed)..."
echo "Vérifier : user_id ABSENT, citizen_id_secure PRÉSENT"
echo "Vérifier : sentiment_score et categorie PRÉSENTS"
docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic social_analyzed \
    --from-beginning \
    --max-messages 3 \
    --timeout-ms 10000

# ── 5. Déclencher une alerte CRISE manuellement ──────────────────────────
echo ""
echo "[5/5] Déclenchement d'une alerte CRISE Wave..."
/home/abasse/vox-sn/venv_vox/bin/python3 << 'PYEOF'
from kafka import KafkaProducer
import json, uuid
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)
for i in range(10):
    post = {
        'post_id': str(uuid.uuid4()),
        'user_id': f'USR_TEST_{i}',
        'phone_number': f'7{i}0000000',
        'service_cible': 'WAVE',
        'texte_du_post': 'Wave dafa teye arnaque total argent perdu transaction echouee !',
        'langue': 'WO',
        'canal': 'TWITTER',
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'region': 'DAKAR'
    }
    producer.send('social_raw', post)
    print(f'Post CRISE {i+1}/10 envoyé')
producer.flush()
print('ALERTE CRISE déclenchée !')
PYEOF

# ── Arrêt du simulateur ───────────────────────────────────────────────────
echo ""
echo "Arrêt du simulateur..."
kill $PRODUCER_PID 2>/dev/null

echo ""
echo "======================================"
echo " TESTS TERMINÉS"
echo "======================================"