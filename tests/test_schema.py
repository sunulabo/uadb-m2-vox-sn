"""
tests/test_schema.py — Tests du contrat Pandera Vox-SN
======================================================
Vérifie la validation des posts citoyens et la détection PII.
"""
from __future__ import annotations

import sys
import os
import pytest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark'))

from schema import (  # noqa: E402
    contains_pii, SocialSentimentSchema, validate_and_filter,
    PII_PATTERNS,
)


# =============================================================================
# Détection PII
# =============================================================================
class TestDetectionPII:
    """Validation de la détection des PII sénégalais."""

    def test_telephone_senegal_complet(self):
        """Format +221 7X XXX XX XX."""
        assert contains_pii("Appelez-moi au +221771234567")
        assert contains_pii("Mon numéro : +221701234567")

    def test_telephone_local(self):
        """Format 7X XXX XX XX."""
        assert contains_pii("Mon mobile 771234567 ne marche pas")
        assert contains_pii("J'ai appelé 781234567")

    def test_numero_compte(self):
        """Numéros de compte/transaction longs."""
        assert contains_pii("Compte 1234567890")
        assert contains_pii("Transaction ID: 123456789012345")

    def test_texte_propre(self):
        """Texte sans PII doit retourner False."""
        assert not contains_pii("Wave dafa teye, problem bi !")
        assert not contains_pii("Service Mobile Money très pratique")
        assert not contains_pii("")

    def test_dates_non_detectees(self):
        """Les dates ne doivent pas être détectées comme PII."""
        # 2025-01-15 = 10 chiffres mais pas tous contigus
        assert not contains_pii("Hier le 15 janvier 2025")


# =============================================================================
# Validation Pandera
# =============================================================================
class TestSchemaPandera:
    """Tests du contrat de données."""

    def _post_valide(self, **overrides):
        base = {
            'post_id': 'POST_001',
            'service_cible': 'WAVE',
            'texte_du_post': 'Wave dafa baax, top !',
            'langue': 'WO',
            'timestamp': '2025-10-15T12:00:00Z',
            'canal': 'TWITTER',
        }
        base.update(overrides)
        return base

    def test_post_valide(self):
        df = pd.DataFrame([self._post_valide()])
        result = validate_and_filter(df, SocialSentimentSchema)
        assert len(result) == 1, "Post valide rejeté à tort"

    def test_service_invalide(self):
        """Un service hors liste blanche est rejeté."""
        df = pd.DataFrame([self._post_valide(service_cible='YOUTUBE')])
        result = validate_and_filter(df, SocialSentimentSchema)
        assert len(result) == 0, "Service invalide accepté à tort"

    def test_langue_invalide(self):
        """Une langue hors {FR,WO,EN} est rejetée."""
        df = pd.DataFrame([self._post_valide(langue='DE')])
        result = validate_and_filter(df, SocialSentimentSchema)
        assert len(result) == 0

    def test_texte_trop_court(self):
        """Texte < 5 caractères rejeté."""
        df = pd.DataFrame([self._post_valide(texte_du_post='Hi')])
        result = validate_and_filter(df, SocialSentimentSchema)
        assert len(result) == 0

    def test_texte_avec_pii_rejete(self):
        """Un texte contenant un numéro de téléphone est rejeté."""
        df = pd.DataFrame([self._post_valide(
            texte_du_post='Mon Wave ne marche pas, appelez 771234567'
        )])
        result = validate_and_filter(df, SocialSentimentSchema)
        assert len(result) == 0, "PII non détecté dans le texte"

    def test_timestamp_invalide(self):
        df = pd.DataFrame([self._post_valide(timestamp='15/10/2025')])
        result = validate_and_filter(df, SocialSentimentSchema)
        assert len(result) == 0

    def test_doublons_post_id(self):
        """post_id en double doit déclencher une erreur."""
        df = pd.DataFrame([
            self._post_valide(post_id='POST_X'),
            self._post_valide(post_id='POST_X', texte_du_post='Autre post'),
        ])
        result = validate_and_filter(df, SocialSentimentSchema)
        assert len(result) < 2, "Doublon post_id accepté à tort"


# =============================================================================
# Mix valides & invalides
# =============================================================================
class TestValidationMixte:
    """Vérifie que le filtrage conserve les valides et rejette les invalides."""

    def test_garde_les_valides(self):
        posts = [
            {'post_id': f'P{i}',
             'service_cible': 'WAVE',
             'texte_du_post': f'Post numéro {i} avec Wave',
             'langue': 'FR',
             'timestamp': '2025-10-15T12:00:00Z',
             'canal': 'TWITTER'}
            for i in range(5)
        ]
        # Un post avec PII
        posts.append({
            'post_id': 'P_BAD',
            'service_cible': 'WAVE',
            'texte_du_post': 'Mon 771234567 ne marche pas',
            'langue': 'FR',
            'timestamp': '2025-10-15T12:00:00Z',
            'canal': 'TWITTER'
        })
        df = pd.DataFrame(posts)
        result = validate_and_filter(df, SocialSentimentSchema)
        # 5 valides + 1 rejeté (PII) = 5 conservés
        assert len(result) == 5
        assert 'P_BAD' not in result['post_id'].values


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
