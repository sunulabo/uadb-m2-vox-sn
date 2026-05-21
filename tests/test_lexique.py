"""
tests/test_lexique.py — Tests unitaires du lexique Vox-SN
=========================================================
Vérifie l'intégrité du lexique sentiment Wolof/Français
et le bon fonctionnement des helpers de scoring.
"""
from __future__ import annotations

import pytest
import sys
import os

# Permettre l'import depuis spark/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark'))

from lexique_sn import (  # noqa: E402
    NEGATIF, POSITIF, CATEGORIES,
    STOPWORDS_WOLOF, STOPWORDS_FR_FINTECH, ALL_STOPWORDS,
)


# =============================================================================
# Lexique — structure & contenu
# =============================================================================
class TestLexiqueStructure:
    """Validation de la structure du lexique."""

    def test_negatif_non_vide(self):
        assert len(NEGATIF) > 0, "Lexique NEGATIF vide"

    def test_positif_non_vide(self):
        assert len(POSITIF) > 0, "Lexique POSITIF vide"

    def test_negatif_scores_negatifs(self):
        """Tous les scores négatifs doivent être < 0."""
        for terme, score in NEGATIF.items():
            assert score < 0, f"Score positif dans NEGATIF: {terme}={score}"

    def test_positif_scores_positifs(self):
        """Tous les scores positifs doivent être > 0."""
        for terme, score in POSITIF.items():
            assert score > 0, f"Score négatif dans POSITIF: {terme}={score}"

    def test_termes_wolof_presents(self):
        """Le lexique doit contenir des termes Wolof clés."""
        termes_wolof_obligatoires = ['dafa teye', 'dafa baax', 'cher na']
        tous_termes = set(NEGATIF.keys()) | set(POSITIF.keys())
        for terme in termes_wolof_obligatoires:
            assert terme in tous_termes, f"Terme Wolof manquant: {terme}"

    def test_termes_fintech_presents(self):
        """Le lexique doit contenir des termes Fintech."""
        termes_fintech = ['arnaque', 'panne', 'remboursement']
        for terme in termes_fintech:
            assert terme in NEGATIF, f"Terme Fintech manquant: {terme}"


# =============================================================================
# Catégories
# =============================================================================
class TestCategories:
    """Validation des catégories de plaintes."""

    def test_categories_obligatoires(self):
        """Les 4 catégories métier obligatoires existent."""
        for cat in ['TARIF', 'TECHNIQUE', 'FRAUDE', 'SERVICE_CLIENT']:
            assert cat in CATEGORIES, f"Catégorie manquante: {cat}"

    def test_categories_non_vides(self):
        """Chaque catégorie contient au moins 1 mot-clé."""
        for cat, mots in CATEGORIES.items():
            assert len(mots) > 0, f"Catégorie vide: {cat}"


# =============================================================================
# Stopwords
# =============================================================================
class TestStopwords:
    """Validation des stopwords."""

    def test_stopwords_wolof_present(self):
        assert 'ak' in STOPWORDS_WOLOF
        assert 'bi' in STOPWORDS_WOLOF

    def test_stopwords_fintech_present(self):
        assert 'wave' in STOPWORDS_FR_FINTECH

    def test_all_stopwords_fusionnes(self):
        """ALL_STOPWORDS contient l'union des deux ensembles."""
        assert STOPWORDS_WOLOF.issubset(ALL_STOPWORDS)
        assert STOPWORDS_FR_FINTECH.issubset(ALL_STOPWORDS)


# =============================================================================
# Scoring simulé (réplique de l'UDF)
# =============================================================================
def score_sentiment_simule(texte: str) -> float:
    """Réplique simplifiée de l'UDF Spark pour test."""
    if not texte:
        return 0.0
    t = texte.lower()
    score = 0.0
    count = 0
    for terme, val in NEGATIF.items():
        if terme in t:
            score += val
            count += 1
    for terme, val in POSITIF.items():
        if terme in t:
            score += val
            count += 1
    return score / max(count, 1)


class TestScoring:
    """Vérifie le scoring sur des phrases types."""

    def test_post_negatif_wolof(self):
        score = score_sentiment_simule("Wave dafa teye, problem bi")
        assert score < -0.5, f"Score insuffisamment négatif: {score}"

    def test_post_positif_wolof(self):
        score = score_sentiment_simule("Wave dafa baax, dafa yomb")
        assert score > 0.5, f"Score insuffisamment positif: {score}"

    def test_post_fraude(self):
        score = score_sentiment_simule("Wave c'est une arnaque, argent perdu")
        assert score < -0.7, f"Fraude pas détectée: {score}"

    def test_texte_vide(self):
        assert score_sentiment_simule("") == 0.0
        assert score_sentiment_simule(None) == 0.0

    def test_texte_neutre(self):
        score = score_sentiment_simule("Bonjour comment allez-vous")
        assert -0.3 <= score <= 0.3, f"Texte neutre mal scoré: {score}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
