#!/bin/bash
# test_hive.sh — Vérification des vues Hive Vox-SN
# Teste les 3 vues analytics : Battle MM, Alertes Crises, Parts de Voix

echo "======================================"
echo " TEST VUES HIVE VOX-SN"
echo "======================================"

# ── 1. Battle Mobile Money ────────────────────────────────────────────────
echo ""
echo "[1/3] Battle Mobile Money (Wave vs Orange Money vs Free Money)..."
docker exec hive-metastore beeline -u jdbc:hive2://localhost:10000 \
    -e 'SELECT * FROM vox_sn.vue_battle_mobile_money;'

# ── 2. Alertes de crise ───────────────────────────────────────────────────
echo ""
echo "[2/3] Alertes de crise actives..."
docker exec hive-metastore beeline -u jdbc:hive2://localhost:10000 \
    -e 'SELECT * FROM vox_sn.vue_alertes_crises;'

# ── 3. Parts de voix ─────────────────────────────────────────────────────
echo ""
echo "[3/3] Parts de voix par service (30 jours)..."
docker exec hive-metastore beeline -u jdbc:hive2://localhost:10000 \
    -e 'SELECT * FROM vox_sn.vue_parts_de_voix;'

echo ""
echo "======================================"
echo " TESTS HIVE TERMINÉS"
echo "======================================"