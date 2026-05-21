"""
kafka_producer_vox.py — Simulateur de posts citoyens Vox-SN
============================================================

Génère des posts réalistes en français, wolof et anglais sur les 6 services
ciblés. Chaque service a des probabilités de sentiment négatif calibrées
sur la réalité du terrain :
    - SENELEC      : 65% négatif (coupures fréquentes)
    - SEN_EAU      : 70% négatif
    - WAVE         : 35% négatif (service jugé performant)
    - ORANGE_MONEY : 55% négatif
    - FREE_MONEY   : 60% négatif (jeune service, bugs)
    - TER          : 45% négatif

⚠️  ATTENTION PRIVACY ⚠️
Les champs user_id et phone_number sont VOLONTAIREMENT présents pour
simuler la réalité d'une collecte sociale. Ils seront SUPPRIMÉS par la
Privacy Layer du pipeline Spark Streaming (cf. streaming_sentiment.py).

Usage :
    python kafka_producer_vox.py
    python kafka_producer_vox.py --rate 1.0 --total 1000

Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
Auteur    : Vox-SN Team - UADB M2 BD&IA 2025-2026
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

# -----------------------------------------------------------------------------
# Configuration logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("VoxProducer")


# =============================================================================
# Catalogue des services & canaux
# =============================================================================
SERVICES: list[str] = [
    "SENELEC", "SEN_EAU", "TER",
    "WAVE", "ORANGE_MONEY", "FREE_MONEY",
]

CANAUX: list[str] = ["TWITTER", "FACEBOOK", "WHATSAPP", "RECLAMATION"]

REGIONS_SN: list[str] = [
    "DAKAR", "THIES", "KAOLACK", "SAINT_LOUIS",
    "ZIGUINCHOR", "DIOURBEL", "TAMBACOUNDA", "MATAM",
]


# =============================================================================
# Templates de posts réalistes par service
# Format : (langue, texte)
# Posts soigneusement écrits SANS PII dans le contenu (le PII est dans user_id)
# =============================================================================
TEMPLATES_POSTS: dict[str, list[tuple[str, str]]] = {
    "WAVE": [
        ("FR", "Mon transfert Wave est bloqué depuis 2h, impossible de joindre le support !"),
        ("WO", "Wave dafa teye, duma gënn xaalis bi !"),
        ("FR", "Wave rapide et pratique, meilleur service Mobile Money du Sénégal"),
        ("FR", "Frais Wave trop cher na ! Orange Money moins cher"),
        ("WO", "Wave dafa baax, dafa yomb"),
        ("FR", "Transaction Wave échouée, argent débité sans confirmation"),
        ("FR", "Wave excellent service, je recommande à tous"),
        ("FR", "Encore une fois Wave en panne, c'est inadmissible"),
        ("EN", "Wave is the best mobile money service in Senegal, fast and reliable"),
        ("FR", "Wave m'a remboursé en 24h, top !"),
    ],
    "ORANGE_MONEY": [
        ("FR", "Orange Money encore en panne ce matin, inacceptable !"),
        ("FR", "Les frais cachés Orange Money, c'est de l'arnaque !"),
        ("WO", "Orange Money cher na trop, dafa neka"),
        ("FR", "Remboursement Orange Money jamais reçu après 3 semaines"),
        ("FR", "Orange Money fiable pour les transferts internationaux"),
        ("FR", "Orange Money compte bloqué sans préavis, scandaleux"),
        ("WO", "Orange Money dafa teye, problem bi nekkul ko"),
        ("FR", "Service Orange Money efficace pour les paiements de factures"),
        ("EN", "Orange Money fraud alert, lost my money this morning"),
    ],
    "FREE_MONEY": [
        ("FR", "Free Money nouveau service, encore beaucoup de bugs"),
        ("FR", "Free Money gratuit pour les transferts entre abonnés Free"),
        ("WO", "Free Money problem bi, duma gënn"),
        ("FR", "Compte Free Money bloqué sans explication du support"),
        ("FR", "Free Money simple et pratique pour les petites sommes"),
        ("WO", "Free Money dafa baax mais sa support dafa teye"),
        ("FR", "Free Money lent par rapport à Wave, à améliorer"),
    ],
    "SENELEC": [
        ("FR", "Coupure Senelec depuis 6h à Pikine, quand est-ce que ça revient ?"),
        ("FR", "Facture Senelec incompréhensible, frais anormaux ce mois"),
        ("WO", "Senelec dafa teye, xam dina tax !"),
        ("FR", "Application Senelec bien améliorée, paiement facile maintenant"),
        ("FR", "Senelec délestage tous les jours, ras le bol"),
        ("WO", "Senelec problem bi, dafa metti"),
        ("FR", "Senelec a réparé rapidement la panne dans mon quartier, bravo"),
        ("FR", "Coupure électrique pendant le ramadan, Senelec scandaleux"),
    ],
    "SEN_EAU": [
        ("FR", "Pas d'eau depuis 3 jours à Guédiawaye, Sen Eau ne répond pas"),
        ("FR", "Sen Eau pression faible depuis une semaine, scandaleux"),
        ("WO", "Sen Eau problem bi, dafa neka"),
        ("FR", "Facture Sen Eau anormalement élevée ce mois-ci"),
        ("FR", "Sen Eau service client injoignable, attente interminable"),
        ("FR", "L'eau est revenue après 2 jours, Sen Eau enfin réactif"),
    ],
    "TER": [
        ("FR", "TER en retard encore aujourd'hui, infos en temps réel impossible"),
        ("FR", "TER pratique et rapide entre Dakar et Diamniadio, top !"),
        ("WO", "TER dafa baax, rafet"),
        ("FR", "TER bondé aux heures de pointe, prévoir extension nécessaire"),
        ("FR", "TER ponctuel ce matin, voyage confortable"),
        ("FR", "Application TER bug encore, impossible d'acheter un ticket"),
    ],
}


# Probabilité d'avoir un post négatif par service (calibrée terrain)
PROB_NEGATIF: dict[str, float] = {
    "WAVE": 0.35,
    "ORANGE_MONEY": 0.55,
    "FREE_MONEY": 0.60,
    "SENELEC": 0.65,
    "SEN_EAU": 0.70,
    "TER": 0.45,
}


# =============================================================================
# Fabrique de posts
# =============================================================================
def _is_negative(text: str) -> bool:
    """Heuristique simple pour détecter un post négatif depuis son contenu."""
    keywords_neg = [
        "panne", "blocage", "bloqué", "arnaque", "cher", "frais",
        "teye", "problem", "neka", "metti", "perdu", "scandal",
        "marre", "ras le bol", "scam", "fraud", "broken",
    ]
    txt_low = text.lower()
    return any(kw in txt_low for kw in keywords_neg)


def gen_post(service: str | None = None) -> dict[str, Any]:
    """
    Génère un post citoyen pseudo-aléatoire.

    Le mécanisme de filtrage négatif/positif respecte les probabilités
    `PROB_NEGATIF` afin de reproduire un mix réaliste.

    Parameters
    ----------
    service : str | None
        Service ciblé. Si None, choisi aléatoirement.

    Returns
    -------
    dict
        Post au format attendu par le topic Kafka `social_raw`.
    """
    if service is None:
        service = random.choice(SERVICES)

    templates = TEMPLATES_POSTS.get(service, [])
    if not templates:
        langue, texte = "FR", f"Avis sur {service}"
    else:
        # Stratégie de tirage pondérée selon PROB_NEGATIF
        prob_neg = PROB_NEGATIF.get(service, 0.50)
        roll = random.random()

        neg_templates = [t for t in templates if _is_negative(t[1])]
        pos_templates = [t for t in templates if not _is_negative(t[1])]

        if roll < prob_neg and neg_templates:
            langue, texte = random.choice(neg_templates)
        elif pos_templates:
            langue, texte = random.choice(pos_templates)
        else:
            langue, texte = random.choice(templates)

    return {
        "post_id": str(uuid.uuid4()),
        "user_id": f"USR_{uuid.uuid4().hex[:10].upper()}",          # Sera anonymisé
        "phone_number": f"7{random.randint(10000000, 99999999)}",   # Sera supprimé
        "service_cible": service,
        "texte_du_post": texte,
        "langue": langue,
        "canal": random.choice(CANAUX),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region": random.choice(REGIONS_SN),
    }


# =============================================================================
# Producteur Kafka
# =============================================================================
def build_producer(brokers: str = "kafka:9092", retries: int = 5) -> KafkaProducer:
    """Construit un KafkaProducer robuste (JSON UTF-8)."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=brokers.split(","),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=10,
            )
            logger.info("KafkaProducer connecté à %s", brokers)
            return producer
        except KafkaError as exc:
            logger.warning(
                "Échec connexion Kafka (tentative %d/%d) : %s",
                attempt + 1, retries, exc,
            )
            time.sleep(5)
    raise RuntimeError(f"Impossible de se connecter à Kafka après {retries} tentatives.")


# =============================================================================
# Boucle principale
# =============================================================================
def run(brokers: str, topic: str, rate_seconds: float, total: int | None) -> None:
    """Démarre la boucle de production."""
    producer = build_producer(brokers)

    # Gestion propre Ctrl+C
    def _graceful_shutdown(signum, frame):  # noqa: ARG001
        logger.info("Arrêt demandé, flush en cours...")
        producer.flush(timeout=10)
        producer.close(timeout=10)
        sys.exit(0)

    signal.signal(signal.SIGINT, _graceful_shutdown)
    signal.signal(signal.SIGTERM, _graceful_shutdown)

    logger.info("Simulateur Vox-SN démarré sur topic '%s' à %ss/post", topic, rate_seconds)
    count = 0
    while total is None or count < total:
        post = gen_post()
        try:
            producer.send(topic, post)
            count += 1
            logger.info(
                "→ [%s] [%s] [%-3s] %s",
                post["service_cible"],
                post["langue"],
                post["canal"][:3],
                post["texte_du_post"][:60],
            )
        except KafkaError as exc:
            logger.error("Échec envoi : %s", exc)
        if count % 20 == 0:
            producer.flush()
        time.sleep(rate_seconds)

    producer.flush()
    producer.close()
    logger.info("Production terminée : %d post(s) envoyé(s)", count)


# =============================================================================
# Entrée CLI
# =============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulateur Vox-SN")
    parser.add_argument(
        "--brokers",
        default=os.environ.get("KAFKA_BROKERS", "kafka:9092"),
        help="Liste des brokers Kafka (csv).",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC_RAW", "social_raw"),
        help="Topic Kafka cible.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("PRODUCER_RATE_SECONDS", "2")),
        help="Délai entre 2 posts (secondes).",
    )
    parser.add_argument(
        "--total",
        type=int,
        default=None,
        help="Nombre total de posts à produire (illimité si non spécifié).",
    )
    args = parser.parse_args()

    run(args.brokers, args.topic, args.rate, args.total)
