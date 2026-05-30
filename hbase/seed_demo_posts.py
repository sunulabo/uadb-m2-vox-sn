"""
seed_demo_posts.py — Charge des posts anonymisés dans vox:posts (démo soutenance)
=================================================================================

Simule la sortie du pipeline Spark : SHA-256 sur user_id, suppression de user_id
et phone_number avant écriture HBase.

Usage :
    HBASE_HOST=localhost python hbase/seed_demo_posts.py
    HBASE_HOST=localhost python hbase/seed_demo_posts.py --rows 25
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import sys
from pathlib import Path

import happybase

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import importlib.util

_producer_path = ROOT / "kafka" / "kafka_producer_vox.py"
_spec = importlib.util.spec_from_file_location("kafka_producer_vox", _producer_path)
_mod = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_mod)
gen_post = _mod.gen_post

from spark.lexique_sn import CATEGORIES, NEGATIF, POSITIF  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("HBaseSeed")

HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_THRIFT_PORT", "9090"))
SALT = os.environ.get("CITIZEN_SECRET_SALT", "UADB_VOX_2025")
CRISIS_THRESHOLD = -0.5


def citizen_id_secure(user_id: str) -> str:
    return hashlib.sha256(f"{user_id}{SALT}".encode()).hexdigest()


def score_sentiment(texte: str) -> float:
    if not texte:
        return 0.0
    texte_lower = texte.lower()
    score = 0.0
    matches = 0
    for terme, val in NEGATIF.items():
        if terme in texte_lower:
            score += val
            matches += 1
    for terme, val in POSITIF.items():
        if terme in texte_lower:
            score += val
            matches += 1
    if matches == 0:
        return 0.0
    avg = score / matches
    return float(max(-1.0, min(1.0, avg)))


def categoriser(texte: str) -> str:
    if not texte:
        return "AUTRE"
    texte_lower = texte.lower()
    scores = {
        cat: sum(1 for kw in mots if kw in texte_lower)
        for cat, mots in CATEGORIES.items()
    }
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "AUTRE"


def sentiment_label(score: float) -> str:
    if score < CRISIS_THRESHOLD:
        return "NEGATIF_FORT"
    if score < 0.0:
        return "NEGATIF"
    if score > 0.3:
        return "POSITIF"
    return "NEUTRE"


def statut_alerte(score: float, categorie: str) -> str:
    if score < CRISIS_THRESHOLD and categorie in ("FRAUDE", "TECHNIQUE"):
        return "CRISE"
    if score < CRISIS_THRESHOLD:
        return "NEGATIF_FORT"
    return "NORMAL"


def seed_rows(rows: int) -> int:
    conn = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, timeout=10000)
    table = conn.table(b"vox:posts")
    batch = table.batch()

    for i in range(1, rows + 1):
        raw = gen_post()
        row_key = f"POST_{i:06d}"
        score = score_sentiment(raw["texte_du_post"])
        cat = categoriser(raw["texte_du_post"])

        batch.put(
            row_key.encode(),
            {
                b"meta:post_id": row_key.encode(),
                b"meta:service_cible": raw["service_cible"].encode(),
                b"meta:langue": raw["langue"].encode(),
                b"meta:canal": raw["canal"].encode(),
                b"meta:region": raw["region"].encode(),
                b"meta:timestamp": raw["timestamp"].encode(),
                b"nlp:texte_clean": raw["texte_du_post"].lower().encode(),
                b"nlp:sentiment_score": f"{score:.3f}".encode(),
                b"nlp:categorie": cat.encode(),
                b"nlp:sentiment_label": sentiment_label(score).encode(),
                b"nlp:statut_alerte": statut_alerte(score, cat).encode(),
                b"privacy:citizen_id_secure": citizen_id_secure(raw["user_id"]).encode(),
            },
        )

    batch.send()
    conn.close()
    logger.info("✅ %d posts anonymisés chargés dans vox:posts", rows)
    logger.info("   Exemple : get 'vox:posts', 'POST_000001'")
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=20)
    args = parser.parse_args()
    try:
        seed_rows(args.rows)
    except Exception as exc:  # noqa: BLE001
        logger.error("Seed échoué : %s", exc)
        sys.exit(1)
