"""
kafka_consumer_check.py — Vérification rapide des topics Vox-SN
===============================================================

Outil de monitoring CLI pour s'assurer que :
    - les messages arrivent bien dans `social_raw`
    - le pipeline Spark écrit bien dans `social_analyzed`
    - les PII sont absents des messages analysés
    - les agrégats horaires arrivent dans `social_sentiment_agg`

Usage :
    python kafka_consumer_check.py --topic social_raw --max 5
    python kafka_consumer_check.py --topic social_analyzed --check-pii

Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys

from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-7s | %(message)s",
)
logger = logging.getLogger("VoxConsumerCheck")


# PII forbidden in social_analyzed
PII_FORBIDDEN = {"user_id", "phone_number"}

PII_REGEX = [
    re.compile(r"\+?221[0-9]{9}"),
    re.compile(r"\b7[0-9]{8}\b"),
    re.compile(r"\b[0-9]{10,16}\b"),
]


def check_record_privacy(record: dict) -> list[str]:
    """Retourne la liste des violations Privacy détectées dans un record."""
    violations: list[str] = []
    for forbidden in PII_FORBIDDEN:
        if forbidden in record:
            violations.append(f"champ interdit présent : '{forbidden}'")

    # Vérification regex dans tous les champs string
    for k, v in record.items():
        if isinstance(v, str):
            for regex in PII_REGEX:
                if regex.search(v):
                    violations.append(f"PII détecté dans '{k}' : {regex.pattern}")
                    break
    return violations


def main() -> int:
    parser = argparse.ArgumentParser(description="Consommateur de contrôle Vox-SN")
    parser.add_argument(
        "--brokers",
        default=os.environ.get("KAFKA_BROKERS", "kafka:9092"),
    )
    parser.add_argument("--topic", required=True)
    parser.add_argument("--max", type=int, default=10, help="Messages à consommer")
    parser.add_argument(
        "--check-pii",
        action="store_true",
        help="Active la vérification Privacy",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Lit depuis le début du topic",
    )
    args = parser.parse_args()

    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.brokers.split(","),
            auto_offset_reset="earliest" if args.from_beginning else "latest",
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=30000,
            group_id=None,  # Lecture standalone
        )
    except KafkaError as exc:
        logger.error("Connexion Kafka échouée : %s", exc)
        return 1

    logger.info("Lecture de %s (max %d msgs)...", args.topic, args.max)
    count = 0
    violations_total = 0
    for msg in consumer:
        count += 1
        record = msg.value
        logger.info("--- Message #%d ---", count)
        logger.info(json.dumps(record, ensure_ascii=False, indent=2))

        if args.check_pii:
            violations = check_record_privacy(record)
            if violations:
                violations_total += len(violations)
                for v in violations:
                    logger.error("⚠️  VIOLATION PRIVACY : %s", v)
            else:
                logger.info("✅ Aucune violation Privacy")

        if count >= args.max:
            break

    consumer.close()
    logger.info("Total lu : %d message(s)", count)
    if args.check_pii:
        if violations_total > 0:
            logger.error("❌ %d violation(s) Privacy détectée(s) !", violations_total)
            return 2
        logger.info("✅ Aucune violation Privacy sur les %d messages", count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
