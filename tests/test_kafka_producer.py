"""
tests/test_kafka_producer.py — Tests du simulateur de posts
===========================================================
Vérifie la qualité des posts générés par kafka_producer_vox.
"""
from __future__ import annotations

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'kafka'))

# Import sans connecter à Kafka (le producteur global ne sera pas instancié)
import importlib.util  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "kpv",
    os.path.join(os.path.dirname(__file__), '..', 'kafka', 'kafka_producer_vox.py')
)


class TestSimulateurPosts:
    """Vérifie la qualité des posts générés."""

    @pytest.fixture(scope='class', autouse=True)
    def patch_kafka(self, monkeypatch_class):
        """Stubber kafka pour éviter une connexion réelle."""
        # Pas de réseau dans les tests : on importe avec un mock léger
        pass

    @pytest.fixture(scope='class')
    def gen_post(self):
        """Importe gen_post sans déclencher KafkaProducer."""
        import sys
        import types
        import unittest.mock as mock

        mock_kafka = types.ModuleType("kafka")
        mock_kafka.KafkaProducer = mock.MagicMock()
        mock_errors = types.ModuleType("kafka.errors")
        mock_errors.KafkaError = Exception

        with mock.patch.dict(
            sys.modules,
            {
                "kafka": mock_kafka,
                "kafka.errors": mock_errors,
            },
        ):
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            yield mod.gen_post

    def test_structure_post(self, gen_post):
        """Le post contient tous les champs obligatoires."""
        post = gen_post()
        champs = {'post_id', 'user_id', 'phone_number', 'service_cible',
                  'texte_du_post', 'langue', 'canal', 'timestamp', 'region'}
        assert champs.issubset(post.keys())

    def test_service_valide(self, gen_post):
        services_ok = {'SENELEC', 'SEN_EAU', 'TER',
                       'WAVE', 'ORANGE_MONEY', 'FREE_MONEY'}
        for _ in range(20):
            post = gen_post()
            assert post['service_cible'] in services_ok

    def test_langue_valide(self, gen_post):
        for _ in range(20):
            post = gen_post()
            assert post['langue'] in {'FR', 'WO', 'EN'}

    def test_texte_non_vide(self, gen_post):
        for _ in range(20):
            post = gen_post()
            assert len(post['texte_du_post']) > 5

    def test_post_id_unique(self, gen_post):
        ids = {gen_post()['post_id'] for _ in range(100)}
        assert len(ids) == 100, "Collision de post_id détectée"


# Fixture utilitaire pour le test "class scope monkeypatch"
@pytest.fixture(scope='class')
def monkeypatch_class(request):
    from _pytest.monkeypatch import MonkeyPatch
    mp = MonkeyPatch()
    request.addfinalizer(mp.undo)
    return mp


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
