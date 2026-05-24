# lexique_sn.py — Lexique de sentiment Wolof/Français pour Vox-SN
# Spécificités sénégalaises : termes Wolof + argot Fintech local

# ── Termes négatifs ──────────────────────────────────────────────────────
NEGATIF = {
    # Wolof
    'dafa teye': -0.9,    # ça ne marche pas
    'cher na': -0.7,      # c'est cher
    'dafa neka': -0.8,    # c'est nul
    'problem bi': -0.6,   # le problème
    'douma gënn': -0.8,   # je n'arrive pas
    'xam dina tax': -0.7, # exaspération
    'guiss dafa': -0.5,   # voir que c'est mauvais
    # Français local
    'arnaque': -0.95,
    'escroquerie': -0.95,
    'panne': -0.8,
    'indisponible': -0.8,
    'frais cachés': -0.85,
    'trop cher': -0.75,
    'remboursement': -0.6,
    'bloqué': -0.7,
    'impossible': -0.65,
    'nul': -0.7,
    'honte': -0.8,
    'vol': -0.9,
    'transaction échouée': -0.85,
    'argent perdu': -1.0,
}

# ── Termes positifs ──────────────────────────────────────────────────────
POSITIF = {
    # Wolof
    'dafa baax': +0.9,  # c'est bien
    'jaama': +0.7,      # paix / satisfaction
    'rafet': +0.8,      # beau / bien
    'soo cool': +0.7,   # très cool
    'dafa yomb': +0.8,  # c'est facile
    # Français local
    'rapide': +0.7,
    'pratique': +0.75,
    'gratuit': +0.8,
    'simple': +0.7,
    'merci': +0.6,
    'excellent': +0.9,
    'top': +0.8,
    'fiable': +0.85,
}

# ── Catégories de plaintes ───────────────────────────────────────────────
CATEGORIES = {
    'TARIF': ['cher', 'frais', 'coût', 'prix', 'commission', 'cher na'],
    'TECHNIQUE': ['panne', 'teye', 'bloqué', 'indisponible', 'lent', 'bug'],
    'FRAUDE': ['arnaque', 'escroquerie', 'vol', 'argent perdu', 'scam'],
    'SERVICE_CLIENT': ['réponse', 'attente', 'support', 'aide', 'joindre'],
    'POSITIF': list(POSITIF.keys()),
}

# ── Stopwords Wolof à filtrer ─────────────────────────────────────────────
STOPWORDS_WOLOF = {
    'ak','bi','gi','yi','mi','ci','di','na','la','ma','da',
    'nga','bu','ni','si','fi','woon','doon','nit','ñi',
}

STOPWORDS_FR_FINTECH = {
    'bonjour','bonsoir','svp','stp','merci','ok','oui','non',
    'wave','orange','free','money','mobile','senegal','sn',
}

ALL_STOPWORDS = STOPWORDS_WOLOF | STOPWORDS_FR_FINTECH

if __name__ == '__main__':
    print(f'Termes négatifs : {len(NEGATIF)}')
    print(f'Termes positifs : {len(POSITIF)}')
    print(f'Catégories : {list(CATEGORIES.keys())}')
    print(f'Stopwords total : {len(ALL_STOPWORDS)}')
    print('lexique_sn.py OK')