"""
lexique_sn.py — Lexique de sentiment Wolof/Français pour Vox-SN
================================================================

Cœur sémantique du projet. Ce lexique constitue la valeur ajoutée
DIFFÉRENCIANTE de Vox-SN : il couvre les termes Wolof spécifiques au
Sénégal (« dafa teye » = ça ne marche pas, « cher na » = c'est cher)
ainsi que les plaintes typiques des utilisateurs de Mobile Money en
Afrique de l'Ouest.

Structure :
    - NEGATIF / POSITIF : dictionnaires {terme: score in [-1, +1]}
    - CATEGORIES        : taxonomie des plaintes (Tarif/Technique/Fraude/Service)
    - STOPWORDS         : mots à filtrer (Wolof + français Fintech)

Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
Auteur    : Vox-SN Team - UADB M2 BD&IA 2025-2026
"""

from __future__ import annotations

from typing import Final


# =============================================================================
# 1. TERMES NÉGATIFS
# =============================================================================
NEGATIF: Final[dict[str, float]] = {
    # --- Wolof ---
    "dafa teye":      -0.90,  # ça ne marche pas
    "cher na":        -0.70,  # c'est cher
    "dafa neka":      -0.80,  # c'est nul
    "problem bi":     -0.60,  # le problème
    "douma genn":     -0.80,  # je n'arrive pas (sans diacritiques)
    "douma gënn":     -0.80,  # je n'arrive pas
    "xam dina tax":   -0.70,  # exaspération
    "guiss dafa":     -0.50,  # voir que c'est mauvais
    "neexul":         -0.70,  # ce n'est pas agréable
    "ñaaw":           -0.80,  # mauvais / laid
    "dafa metti":     -0.75,  # c'est dur / pénible

    # --- Français local Fintech ---
    "arnaque":            -0.95,
    "escroquerie":        -0.95,
    "panne":              -0.80,
    "indisponible":       -0.80,
    "frais cachés":       -0.85,
    "trop cher":          -0.75,
    "remboursement":      -0.60,
    "bloqué":             -0.70,
    "impossible":         -0.65,
    "nul":                -0.70,
    "honte":              -0.80,
    "vol":                -0.90,
    "transaction échouée": -0.85,
    "argent perdu":       -1.00,
    "scandaleux":         -0.85,
    "inacceptable":       -0.85,
    "lent":               -0.50,
    "bug":                -0.60,
    "déçu":               -0.65,
    "décevant":           -0.70,
    "marre":              -0.65,
    "ras le bol":         -0.75,
    "scam":               -0.95,
    "voleur":             -0.90,
    "rien ne marche":     -0.85,

    # --- Anglais (posts en EN) ---
    "scam":               -0.95,
    "fraud":              -0.95,
    "broken":             -0.70,
    "useless":            -0.75,
    "down":               -0.50,
}


# =============================================================================
# 2. TERMES POSITIFS
# =============================================================================
POSITIF: Final[dict[str, float]] = {
    # --- Wolof ---
    "dafa baax":      +0.90,  # c'est bien
    "jaama":          +0.70,  # paix / satisfaction
    "rafet":          +0.80,  # beau / bien
    "soo cool":       +0.70,  # très cool
    "dafa yomb":      +0.80,  # c'est facile
    "neex na":        +0.75,  # agréable
    "jëkk":           +0.65,  # parfait

    # --- Français local ---
    "rapide":         +0.70,
    "pratique":       +0.75,
    "gratuit":        +0.80,
    "simple":         +0.70,
    "merci":          +0.60,
    "excellent":      +0.90,
    "top":            +0.80,
    "fiable":         +0.85,
    "efficace":       +0.75,
    "satisfait":      +0.80,
    "génial":         +0.85,
    "parfait":        +0.90,
    "bravo":          +0.80,
    "bien amélioré":  +0.70,
    "meilleur":       +0.75,

    # --- Anglais ---
    "great":          +0.85,
    "awesome":        +0.90,
    "love":           +0.85,
    "fast":           +0.70,
}


# =============================================================================
# 3. CATÉGORIES DE PLAINTES (taxonomie Vox-SN)
# =============================================================================
CATEGORIES: Final[dict[str, list[str]]] = {
    "TARIF": [
        "cher", "frais", "coût", "prix", "commission",
        "cher na", "trop cher", "frais cachés", "augmentation",
    ],
    "TECHNIQUE": [
        "panne", "teye", "bloqué", "indisponible", "lent", "bug",
        "dafa teye", "ne marche pas", "down", "broken",
        "transaction échouée", "rien ne marche",
    ],
    "FRAUDE": [
        "arnaque", "escroquerie", "vol", "argent perdu", "scam",
        "fraud", "voleur", "voleurs",
    ],
    "SERVICE_CLIENT": [
        "réponse", "attente", "support", "aide", "joindre",
        "service client", "agent", "réclamation",
    ],
    "POSITIF": list(POSITIF.keys()),
}


# =============================================================================
# 4. STOPWORDS — Wolof + Français Fintech
# =============================================================================
STOPWORDS_WOLOF: Final[set[str]] = {
    "ak", "bi", "gi", "yi", "mi", "ci", "di", "na", "la", "ma", "da",
    "nga", "bu", "ni", "si", "fi", "woon", "doon", "nit", "ñi",
    "moom", "yow", "nun", "yeen", "ñoom",
}

STOPWORDS_FR_FINTECH: Final[set[str]] = {
    "bonjour", "bonsoir", "svp", "stp", "merci", "ok", "oui", "non",
    "wave", "orange", "free", "money", "mobile", "senegal", "sn",
    "vraiment", "comme", "encore", "toujours", "jamais",
    "le", "la", "les", "un", "une", "des", "et", "ou", "mais", "donc",
    "car", "ni", "puis", "que", "qui", "à", "de", "du", "au",
    "ce", "cette", "ces", "il", "elle", "ils", "elles", "nous", "vous",
    "je", "tu", "on", "se", "ne", "pas", "plus", "moins",
}

ALL_STOPWORDS: Final[set[str]] = STOPWORDS_WOLOF | STOPWORDS_FR_FINTECH


# =============================================================================
# 5. EXPRESSIONS COMPOSÉES (multi-mots à matcher en priorité)
# =============================================================================
# Ces expressions composées sont matchées AVANT le tokenizing pour préserver
# leur sens (ex: "dafa teye" perdrait sa polarité si découpé).
EXPRESSIONS_COMPOSEES: Final[list[str]] = [
    expr for expr in (list(NEGATIF.keys()) + list(POSITIF.keys()))
    if " " in expr
]


# =============================================================================
# Utilitaires
# =============================================================================
def get_all_lexicon() -> dict[str, float]:
    """Retourne le lexique combiné NEGATIF + POSITIF."""
    return {**NEGATIF, **POSITIF}


def get_category_for_text(text: str) -> str:
    """
    Détermine la catégorie majoritaire d'un texte donné.

    Parameters
    ----------
    text : str
        Texte à analyser (peut être bruit, sera lower-cased).

    Returns
    -------
    str
        Nom de catégorie, ou 'AUTRE' si rien ne matche.
    """
    if not text:
        return "INCONNU"
    text_low = text.lower()
    scores = {
        cat: sum(1 for kw in keywords if kw in text_low)
        for cat, keywords in CATEGORIES.items()
    }
    best_cat = max(scores, key=scores.get)
    return best_cat if scores[best_cat] > 0 else "AUTRE"


def compute_sentiment_score(text: str) -> float:
    """
    Calcul du score sentiment lexical d'un texte.

    Returns
    -------
    float
        Score moyen ∈ [-1, +1] des termes matchés, ou 0.0 si aucun.
    """
    if not text:
        return 0.0
    text_low = text.lower()
    score = 0.0
    matches = 0
    combined = get_all_lexicon()
    for term, val in combined.items():
        if term in text_low:
            score += val
            matches += 1
    return float(score / matches) if matches > 0 else 0.0


# =============================================================================
# Auto-test
# =============================================================================
if __name__ == "__main__":
    test_cases = [
        "Wave dafa baax, dafa yomb",                # WO positif
        "Mon transfert est bloqué, c'est une arnaque !",  # FR négatif
        "Orange Money cher na trop, dafa neka",     # WO négatif mixte
        "TER pratique et rapide, top !",            # FR positif
        "Bonjour, comment ça va ?",                 # Neutre
    ]
    print(f"{'Texte':<50} | {'Score':>6} | {'Catégorie':<15}")
    print("-" * 80)
    for txt in test_cases:
        s = compute_sentiment_score(txt)
        c = get_category_for_text(txt)
        print(f"{txt[:50]:<50} | {s:>6.2f} | {c:<15}")
