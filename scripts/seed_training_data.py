"""
scripts/seed_training_data.py — Génération de données d'entraînement
====================================================================
Crée un CSV avec des posts pré-labellisés pour entraîner les modèles ML
sans dépendre du streaming Kafka.

Usage :
    python scripts/seed_training_data.py
    python scripts/seed_training_data.py --rows 5000 --output data/samples/train.csv
"""
from __future__ import annotations

import argparse
import csv
import random
import sys
import os
import uuid
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'kafka'))
from kafka_producer_vox import TEMPLATES_POSTS, SERVICES, CANAUX, PROB_NEGATIF  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark'))
from lexique_sn import NEGATIF, POSITIF, CATEGORIES  # noqa: E402


def score_lexical(texte: str) -> float:
    """Score sentiment basé sur le lexique."""
    t = texte.lower()
    score, count = 0.0, 0
    for terme, val in NEGATIF.items():
        if terme in t:
            score += val
            count += 1
    for terme, val in POSITIF.items():
        if terme in t:
            score += val
            count += 1
    return float(score / max(count, 1))


def categoriser(texte: str) -> str:
    t = texte.lower()
    scores = {cat: sum(1 for m in mots if m in t) for cat, mots in CATEGORIES.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else 'AUTRE'


def label_sentiment(score: float) -> str:
    if score < -0.5:
        return 'NEGATIF_FORT'
    if score < 0.0:
        return 'NEGATIF'
    if score > 0.3:
        return 'POSITIF'
    return 'NEUTRE'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--rows', type=int, default=2000)
    parser.add_argument('--output', default='data/samples/training_data.csv')
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    now = datetime.utcnow()
    with open(args.output, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow([
            'post_id', 'service_cible', 'texte_clean', 'langue',
            'canal', 'region', 'sentiment_score', 'sentiment_label',
            'categorie', 'date_post'
        ])
        for _ in range(args.rows):
            service = random.choice(SERVICES)
            templates = TEMPLATES_POSTS.get(service, [])
            if not templates:
                continue
            langue, texte = random.choice(templates)
            score = score_lexical(texte) + random.uniform(-0.1, 0.1)
            score = max(-1.0, min(1.0, score))
            sent_label = label_sentiment(score)
            cat = categoriser(texte)
            date_post = (now - timedelta(days=random.randint(0, 29))).strftime('%Y-%m-%d')

            # Nettoyage minimal
            texte_clean = ' '.join(
                t for t in texte.lower().split()
                if len(t) > 2 and t.isalpha()
            )

            w.writerow([
                f'POST_{uuid.uuid4().hex[:10]}',
                service,
                texte_clean,
                langue,
                random.choice(CANAUX),
                random.choice(['DAKAR', 'THIES', 'KAOLACK', 'SAINT_LOUIS', 'ZIGUINCHOR']),
                round(score, 3),
                sent_label,
                cat,
                date_post,
            ])

    print(f'✅ {args.rows} posts générés → {args.output}')
    print('   Pour charger dans Hive :')
    print(f'   docker cp {args.output} vox_hive_server:/tmp/training_data.csv')
    print('   docker exec vox_hive_server beeline -u jdbc:hive2://localhost:10000 -e \\')
    print('     "LOAD DATA LOCAL INPATH \'/tmp/training_data.csv\' INTO TABLE vox_sn.posts_analyses;"')


if __name__ == '__main__':
    main()
