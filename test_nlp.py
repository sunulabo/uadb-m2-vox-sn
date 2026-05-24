# test_nlp.py — Tests unitaires pytest pour Vox-SN
# Teste les UDFs NLP, le lexique et la validation Pandera
# Bonus documentation : +0.5 pt

import pytest
import pandas as pd
import re

# ── Tests du lexique sentiment Wolof/FR ──────────────────────────────────

def test_lexique_negatif_charge():
    """Vérifie que le lexique négatif contient des termes Wolof et FR."""
    from lexique_sn import NEGATIF
    assert len(NEGATIF) > 0, "Lexique négatif vide"
    assert 'dafa teye' in NEGATIF, "Terme Wolof 'dafa teye' manquant"
    assert 'arnaque' in NEGATIF, "Terme FR 'arnaque' manquant"
    assert NEGATIF['dafa teye'] < 0, "Score négatif doit être < 0"

def test_lexique_positif_charge():
    """Vérifie que le lexique positif contient des termes Wolof et FR."""
    from lexique_sn import POSITIF
    assert len(POSITIF) > 0, "Lexique positif vide"
    assert 'dafa baax' in POSITIF, "Terme Wolof 'dafa baax' manquant"
    assert 'rapide' in POSITIF, "Terme FR 'rapide' manquant"
    assert POSITIF['dafa baax'] > 0, "Score positif doit être > 0"

def test_categories_definies():
    """Vérifie que toutes les catégories sont définies."""
    from lexique_sn import CATEGORIES
    categories_attendues = ['TARIF', 'TECHNIQUE', 'FRAUDE', 'SERVICE_CLIENT']
    for cat in categories_attendues:
        assert cat in CATEGORIES, f"Catégorie {cat} manquante"

def test_stopwords_wolof():
    """Vérifie que les stopwords Wolof sont présents."""
    from lexique_sn import STOPWORDS_WOLOF, ALL_STOPWORDS
    assert 'bi' in STOPWORDS_WOLOF, "Stopword Wolof 'bi' manquant"
    assert len(ALL_STOPWORDS) > 0, "ALL_STOPWORDS vide"

# ── Tests du scoring sentiment ────────────────────────────────────────────

def test_score_sentiment_negatif():
    """Un texte avec 'arnaque' doit avoir un score négatif."""
    from lexique_sn import NEGATIF, POSITIF

    def score(texte):
        texte_lower = texte.lower()
        s, c = 0.0, 0
        for t, v in NEGATIF.items():
            if t in texte_lower:
                s += v; c += 1
        for t, v in POSITIF.items():
            if t in texte_lower:
                s += v; c += 1
        return s / max(c, 1)

    assert score("arnaque escroquerie argent perdu") < 0
    assert score("Wave dafa teye impossible") < 0

def test_score_sentiment_positif():
    """Un texte avec 'dafa baax' doit avoir un score positif."""
    from lexique_sn import NEGATIF, POSITIF

    def score(texte):
        texte_lower = texte.lower()
        s, c = 0.0, 0
        for t, v in NEGATIF.items():
            if t in texte_lower:
                s += v; c += 1
        for t, v in POSITIF.items():
            if t in texte_lower:
                s += v; c += 1
        return s / max(c, 1)

    assert score("Wave dafa baax, dafa yomb, rapide") > 0
    assert score("service excellent fiable") > 0

def test_score_sentiment_neutre():
    """Un texte sans termes du lexique doit avoir un score nul."""
    from lexique_sn import NEGATIF, POSITIF

    def score(texte):
        texte_lower = texte.lower()
        s, c = 0.0, 0
        for t, v in NEGATIF.items():
            if t in texte_lower:
                s += v; c += 1
        for t, v in POSITIF.items():
            if t in texte_lower:
                s += v; c += 1
        return s / max(c, 1)

    assert score("bonjour comment allez vous") == 0.0

# ── Tests de détection PII ────────────────────────────────────────────────

def test_detection_numero_senegalais():
    """Vérifie que les numéros sénégalais sont détectés."""
    from schema import contains_pii
    assert contains_pii("+221771234567") == True
    assert contains_pii("Mon numéro est 771234567") == True

def test_detection_numero_transaction():
    """Vérifie que les numéros de transaction sont détectés."""
    from schema import contains_pii
    assert contains_pii("transaction 1234567890123") == True

def test_pas_de_pii_dans_texte_normal():
    """Un texte normal ne doit pas être détecté comme PII."""
    from schema import contains_pii
    assert contains_pii("Wave dafa teye impossible joindre support") == False
    assert contains_pii("Service excellent rapide pratique") == False

# ── Tests de validation Pandera ───────────────────────────────────────────

def test_schema_post_valide():
    """Un post valide doit passer la validation Pandera."""
    from schema import validate_and_filter, SocialSentimentSchema
    df = pd.DataFrame([{
        'post_id': 'TEST001',
        'service_cible': 'WAVE',
        'texte_du_post': 'Wave dafa baax, service rapide',
        'langue': 'WO',
        'timestamp': '2025-01-01T10:00:00Z',
        'canal': 'TWITTER'
    }])
    result = validate_and_filter(df, SocialSentimentSchema)
    assert len(result) == 1, "Post valide rejeté à tort"

def test_schema_rejette_pii():
    """Un post avec PII doit être rejeté par Pandera."""
    from schema import validate_and_filter, SocialSentimentSchema
    df = pd.DataFrame([{
        'post_id': 'TEST002',
        'service_cible': 'WAVE',
        'texte_du_post': 'Mon numéro 771234567 Wave bloqué',
        'langue': 'FR',
        'timestamp': '2025-01-01T10:00:00Z',
        'canal': 'TWITTER'
    }])
    result = validate_and_filter(df, SocialSentimentSchema)
    assert len(result) == 0, "Post avec PII non rejeté"

def test_schema_rejette_service_inconnu():
    """Un service non reconnu doit être rejeté."""
    from schema import validate_and_filter, SocialSentimentSchema
    df = pd.DataFrame([{
        'post_id': 'TEST003',
        'service_cible': 'SERVICE_INCONNU',
        'texte_du_post': 'Texte de test valide ici',
        'langue': 'FR',
        'timestamp': '2025-01-01T10:00:00Z',
        'canal': 'TWITTER'
    }])
    result = validate_and_filter(df, SocialSentimentSchema)
    assert len(result) == 0, "Service inconnu non rejeté"

if __name__ == '__main__':
    pytest.main([__file__, '-v'])