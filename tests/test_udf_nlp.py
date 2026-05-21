"""
tests/test_udf_nlp.py — Tests des UDFs Spark NLP
=================================================
Tests des fonctions de scoring, catégorisation et nettoyage
en mode local (sans cluster Spark).
"""
from __future__ import annotations
import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark'))

from lexique_sn import NEGATIF, POSITIF, CATEGORIES, ALL_STOPWORDS  # noqa: E402


# =============================================================================
# Réplique locale des UDFs (pour pouvoir les tester sans Spark)
# =============================================================================
def score_sentiment(texte: str) -> float:
    if not texte:
        return 0.0
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
    if not texte:
        return 'INCONNU'
    t = texte.lower()
    scores = {cat: sum(1 for m in mots if m in t)
              for cat, mots in CATEGORIES.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else 'AUTRE'


def nettoyer_texte(texte: str) -> str:
    if not texte:
        return ''
    tokens = texte.lower().split()
    tokens = [t for t in tokens
              if t not in ALL_STOPWORDS and len(t) > 2 and t.isalpha()]
    return ' '.join(tokens)


# =============================================================================
# Scoring sentiment
# =============================================================================
class TestScoreSentiment:

    def test_vide(self):
        assert score_sentiment('') == 0.0
        assert score_sentiment(None) == 0.0

    def test_negatif_fort(self):
        s = score_sentiment("argent perdu arnaque escroquerie")
        assert s < -0.8, f"score={s}"

    def test_positif(self):
        s = score_sentiment("Wave dafa baax, rapide et fiable")
        assert s > 0.5, f"score={s}"

    def test_wolof_mix(self):
        s = score_sentiment("Wave dafa teye, douma gënn")
        assert s < -0.5

    def test_neutre(self):
        s = score_sentiment("Hello world test")
        assert -0.1 <= s <= 0.1


# =============================================================================
# Catégorisation
# =============================================================================
class TestCategoriser:

    def test_tarif(self):
        assert categoriser("c'est trop cher, frais excessifs") == 'TARIF'

    def test_technique(self):
        assert categoriser("la panne dure depuis ce matin") == 'TECHNIQUE'

    def test_fraude(self):
        assert categoriser("c'est une arnaque, argent perdu") == 'FRAUDE'

    def test_service_client(self):
        assert categoriser("aucune réponse du support, attente longue") == 'SERVICE_CLIENT'

    def test_autre(self):
        assert categoriser("la météo est belle aujourd'hui") == 'AUTRE'

    def test_vide(self):
        assert categoriser('') == 'INCONNU'


# =============================================================================
# Nettoyage
# =============================================================================
class TestNettoyage:

    def test_stopwords_supprimes(self):
        out = nettoyer_texte("le wave ne marche pas du tout")
        assert 'wave' not in out  # wave est stopword
        assert 'le' not in out

    def test_caracteres_speciaux(self):
        out = nettoyer_texte("hello world !!!")
        # Les tokens isalpha() filtrent ce qui n'est pas purement alphabétique
        assert '!!!' not in out

    def test_minuscules(self):
        out = nettoyer_texte("BONJOUR Service Public")
        # tout doit être en minuscules
        assert out == out.lower()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
