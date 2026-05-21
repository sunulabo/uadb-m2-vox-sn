# -*- coding: utf-8 -*-
"""
================================================================================
 Vox-SN | Injection de Crise (Démo Soutenance)
--------------------------------------------------------------------------------
 UADB | Master 2 Big Data & IA | 2025-2026
 Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Senior Big Data & AI Engineer
                                              Groupe Sonatel
================================================================================

OBJECTIF
--------
Injecte massivement (50 par défaut) des posts NEGATIF_FORT à destination du
topic Kafka `social_raw` afin de déclencher visiblement une ALERTE CRISE en
soutenance.

Ce script est conçu pour la démonstration live :
    1. Lancer le pipeline normal (kafka_producer_vox.py + streaming_sentiment.py)
    2. Exécuter ce script : python inject_crisis.py --service WAVE
    3. Observer dans le dashboard / vue Hive le passage du sentiment Wave
       sous le seuil -0.5 → alerte CRISE visible

USAGE
-----
    python inject_crisis.py                    # 50 posts WAVE négatifs (défaut)
    python inject_crisis.py --service SENELEC --count 100
    python inject_crisis.py --broker kafka:9092 --rate 0.1

PARAMÈTRES
----------
--service : opérateur ciblé (défaut: WAVE)
--count   : nombre de posts à injecter (défaut: 50)
--broker  : broker Kafka (défaut: kafka:9092)
--rate    : pause entre 2 messages en secondes (défaut: 0.1 = très rapide)
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime

try:
    from kafka import KafkaProducer
except ImportError:
    print("ERREUR : kafka-python non installé. pip install kafka-python")
    sys.exit(1)


# ─── Configuration logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] INJECT-CRISIS :: %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('inject_crisis')


# ─── Banque de posts CRISE par opérateur ──────────────────────────────────────
# Posts calibrés pour produire un score < -0.5 (déclenchement statut CRISE)
POSTS_CRISE = {
    'WAVE': [
        ('FR', 'Wave en panne totale depuis 3h, argent perdu, arnaque pure !'),
        ('FR', 'Transaction Wave échouée, débit confirmé, support injoignable'),
        ('WO', 'Wave dafa teye, douma gënn, dafa neka trop !'),
        ('FR', 'Wave bloqué mon compte sans raison, frais cachés scandaleux'),
        ('FR', 'Escroquerie Wave, ils ont volé mon argent, transaction non '
               'remboursée'),
        ('WO', 'Wave problem bi cher na, xam dina tax !'),
        ('FR', 'Panne Wave nationale, impossible de payer mes factures urgentes'),
        ('FR', 'Wave bug grave, transfert indisponible, perdu argent !'),
        ('EN', 'Wave is a scam, my money is gone, transaction failed twice'),
        ('FR', 'Honte à Wave, panne en plein milieu de paiement, débité 50000 '
               'CFA perdus'),
    ],
    'ORANGE_MONEY': [
        ('FR', 'Orange Money panne ce matin, arnaque totale, impossible de '
               'retirer'),
        ('FR', 'Frais cachés Orange Money énormes, escroquerie pure et simple'),
        ('WO', 'Orange Money dafa teye, dafa neka, argent perdu !'),
        ('FR', 'Transaction Orange Money échouée 3 fois, support nul'),
        ('FR', 'Orange Money bloqué mon compte sans explication, scandaleux'),
        ('WO', 'Orange Money problem bi, cher na, douma gënn !'),
        ('FR', 'Vol pur, Orange Money a débité sans raison, remboursement '
               'impossible'),
    ],
    'FREE_MONEY': [
        ('FR', 'Free Money plein de bugs, transaction échouée, argent perdu'),
        ('FR', 'Free Money panne, impossible de transférer, honte !'),
        ('WO', 'Free Money dafa teye, problem bi, dafa neka'),
        ('FR', 'Arnaque Free Money, compte bloqué sans raison'),
        ('FR', 'Free Money frais cachés, escroquerie organisée'),
    ],
    'SENELEC': [
        ('FR', 'Coupure Senelec 8h, scandaleux, plus rien ne marche'),
        ('FR', 'Senelec arnaque, facture incompréhensible, frais cachés'),
        ('WO', 'Senelec dafa teye, xam dina tax, dafa neka !'),
        ('FR', 'Panne Senelec généralisée, honte au service public'),
        ('FR', 'Senelec indisponible depuis 2 jours, impossible de joindre support'),
    ],
    'SEN_EAU': [
        ('FR', 'Pas d eau depuis 5 jours, Sen Eau panne totale, scandaleux'),
        ('FR', 'Sen Eau arnaque, facture élevée pour eau coupée, honte'),
        ('WO', 'Sen Eau dafa teye, problem bi, dafa neka !'),
        ('FR', 'Sen Eau indisponible, impossible de joindre le support'),
    ],
    'TER': [
        ('FR', 'TER en retard 3h, scandaleux, panne récurrente'),
        ('FR', 'TER bloqué, impossible de joindre support, argent perdu billet'),
        ('WO', 'TER dafa teye, xam dina tax !'),
        ('FR', 'TER panne totale, ligne suspendue, honte !'),
    ],
}

CANAUX = ['TWITTER', 'FACEBOOK', 'WHATSAPP', 'RECLAMATION']
REGIONS = ['DAKAR', 'THIES', 'KAOLACK', 'SAINT_LOUIS', 'ZIGUINCHOR']

# Drapeau pour arrêt propre via Ctrl+C
_stop_flag = False


def _handle_sigint(signum, frame):
    """Capture Ctrl+C pour interruption propre."""
    global _stop_flag
    _stop_flag = True
    logger.warning('Interruption demandée — arrêt en cours...')


def build_crisis_post(service: str) -> dict:
    """
    Construit un post de crise calibré pour le service donné.

    Parameters
    ----------
    service : str
        Code service (WAVE, ORANGE_MONEY, FREE_MONEY, SENELEC, SEN_EAU, TER).

    Returns
    -------
    dict
        Post JSON prêt à publier sur Kafka, avec PII (sera supprimé par Spark).
    """
    templates = POSTS_CRISE.get(service, POSTS_CRISE['WAVE'])
    langue, texte = random.choice(templates)
    return {
        'post_id':       str(uuid.uuid4()),
        # user_id et phone_number sont volontairement présents pour valider la
        # Privacy Layer (Spark doit les supprimer). Numéros 100% fictifs.
        'user_id':       f'USR_{uuid.uuid4().hex[:10].upper()}',
        'phone_number':  f'7{random.randint(10000000, 99999999)}',
        'service_cible': service,
        'texte_du_post': texte,
        'langue':        langue,
        'canal':         random.choice(CANAUX),
        'timestamp':     datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'region':        random.choice(REGIONS),
    }


def inject(broker: str, service: str, count: int, rate: float) -> None:
    """
    Boucle d'injection des posts de crise.

    Parameters
    ----------
    broker : str
        Adresse du broker Kafka (host:port).
    service : str
        Service ciblé (clé de POSTS_CRISE).
    count : int
        Nombre de posts à injecter.
    rate : float
        Pause entre 2 envois (en secondes).
    """
    logger.info(f'Connexion Kafka : {broker}')
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=3,
    )
    logger.warning(f'>>> INJECTION CRISE : {count} posts {service} <<<')
    logger.warning('Cette opération est destinée à la démo soutenance.')
    sent = 0
    try:
        for i in range(count):
            if _stop_flag:
                break
            post = build_crisis_post(service)
            producer.send('social_raw', post)
            sent += 1
            if (i + 1) % 10 == 0 or i + 1 == count:
                logger.info(f'  → {i + 1}/{count} posts envoyés '
                            f'(dernier : "{post["texte_du_post"][:55]}...")')
            time.sleep(rate)
        producer.flush(timeout=10)
    finally:
        producer.close(timeout=5)

    logger.info(f'=== INJECTION TERMINEE : {sent}/{count} posts {service} ===')
    logger.info('Surveiller à présent :')
    logger.info('  • Topic social_analyzed (PII supprimés, sentiment scoré)')
    logger.info('  • Vue Hive vue_alertes_crises')
    logger.info('  • Dashboard battle_mm.html')


def main() -> None:
    """CLI principale."""
    parser = argparse.ArgumentParser(
        description='Injection de posts CRISE pour démo soutenance')
    parser.add_argument('--service', default='WAVE',
                        choices=list(POSTS_CRISE.keys()),
                        help='Service ciblé par la crise (défaut: WAVE)')
    parser.add_argument('--count', type=int, default=50,
                        help='Nombre de posts à injecter (défaut: 50)')
    parser.add_argument('--broker', default='kafka:9092',
                        help='Broker Kafka (défaut: kafka:9092)')
    parser.add_argument('--rate', type=float, default=0.1,
                        help='Pause entre 2 messages en sec (défaut: 0.1)')
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)
    signal.signal(signal.SIGTERM, _handle_sigint)

    inject(args.broker, args.service, args.count, args.rate)


if __name__ == '__main__':
    main()
