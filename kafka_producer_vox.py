# kafka_producer_vox.py — Simulateur de posts citoyens Vox-SN
# Génère des posts réalistes en FR/Wolof sur les services publics et Fintech
from kafka import KafkaProducer
import json, random, time, uuid
from datetime import datetime

SERVICES = ['SENELEC','SEN_EAU','TER','WAVE','ORANGE_MONEY','FREE_MONEY']
CANAUX = ['TWITTER','FACEBOOK','WHATSAPP','RECLAMATION']
LANGUES = ['FR','WO','EN']

TEMPLATES_POSTS = {
    'WAVE': [
        ('FR', 'Mon transfert Wave est bloqué depuis 2h, impossible de joindre le support !'),
        ('WO', 'Wave dafa teye, duma gënn xaalis bi !'),
        ('FR', 'Wave rapide et pratique, meilleur service Mobile Money du Sénégal'),
        ('FR', 'Frais Wave trop cher na ! Orange Money moins cher'),
        ('WO', 'Wave dafa baax, dafa yomb'),
        ('FR', 'Transaction Wave échouée, argent débité sans confirmation'),
    ],
    'ORANGE_MONEY': [
        ('FR', 'Orange Money encore en panne ce matin, inacceptable !'),
        ('FR', 'Les frais cachés Orange Money, c est de l arnaque !'),
        ('WO', 'Orange Money cher na trop, dafa neka'),
        ('FR', 'Remboursement Orange Money jamais reçu après 3 semaines'),
        ('FR', 'Orange Money fiable pour les transferts internationaux'),
    ],
    'FREE_MONEY': [
        ('FR', 'Free Money nouveau service, encore beaucoup de bugs'),
        ('FR', 'Free Money gratuit pour les transferts entre abonnés Free'),
        ('WO', 'Free Money problem bi, duma gënn'),
        ('FR', 'Compte Free Money bloqué sans explication du support'),
    ],
    'SENELEC': [
        ('FR', 'Coupure Senelec depuis 6h à Pikine, quand est-ce que ça revient ?'),
        ('FR', 'Facture Senelec incompréhensible, frais anormaux ce mois'),
        ('WO', 'Senelec dafa teye, xam dina tax !'),
        ('FR', 'Application Senelec bien améliorée, paiement facile maintenant'),
    ],
    'SEN_EAU': [
        ('FR', 'Pas d eau depuis 3 jours à Guédiawaye, Sen Eau ne répond pas'),
        ('FR', 'Sen Eau pression faible depuis une semaine, scandaleux'),
        ('WO', 'Sen Eau problem bi, dafa neka'),
    ],
    'TER': [
        ('FR', 'TER en retard encore aujourd hui, infos en temps réel impossible'),
        ('FR', 'TER pratique et rapide entre Dakar et Diamniadio, top !'),
        ('WO', 'TER dafa baax, rafet'),
    ],
}

PROB_NEGATIF = {
    'WAVE': 0.35, 'ORANGE_MONEY': 0.55, 'FREE_MONEY': 0.60,
    'SENELEC': 0.65, 'SEN_EAU': 0.70, 'TER': 0.45,
}

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

def gen_post() -> dict:
    service = random.choice(SERVICES)
    templates = TEMPLATES_POSTS.get(service, [])
    if templates:
        langue, texte = random.choice(templates)
    else:
        langue, texte = 'FR', f'Problème avec {service}'
    return {
        'post_id': str(uuid.uuid4()),
        'user_id': f'USR_{uuid.uuid4().hex[:10].upper()}',
        'phone_number': f'7{random.randint(10000000,99999999)}',
        'service_cible': service,
        'texte_du_post': texte,
        'langue': langue,
        'canal': random.choice(CANAUX),
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'region': random.choice(['DAKAR','THIES','KAOLACK','SAINT_LOUIS','ZIGUINCHOR']),
    }

if __name__ == '__main__':
    print('Simulateur Vox-SN démarré...')
    while True:
        post = gen_post()
        producer.send('social_raw', post)
        print(f'→ [{post["service_cible"]}] [{post["langue"]}] {post["texte_du_post"][:60]}...')
        producer.flush()
        time.sleep(2)