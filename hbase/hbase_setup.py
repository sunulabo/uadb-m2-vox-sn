"""
hbase_setup.py — Création des tables HBase pour Vox-SN
======================================================

Crée trois tables HBase avec column families et TTLs adaptés :

    vox:posts          → posts analysés (post_id en row key)
        meta    : post_id, service, langue
        nlp     : sentiment_score, categorie, mots_cles
        privacy : citizen_id_secure (SHA-256, jamais user_id brut)

    vox:alertes        → alertes crise/fraude (TTL 72h)
        alerte  : type, score, timestamp

    vox:sentiment_agg  → agrégats horaires par opérateur
        stats   : sentiment_moyen, nb_posts, statut

À exécuter UNE FOIS après docker compose up :
    python hbase/hbase_setup.py

Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
Auteur    : Vox-SN Team - UADB M2 BD&IA 2025-2026
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

import happybase


# =============================================================================
# Configuration
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("HBaseSetup")


HBASE_HOST = os.environ.get("HBASE_HOST", "hbase")
HBASE_PORT = int(os.environ.get("HBASE_THRIFT_PORT", "9090"))
HBASE_TIMEOUT_MS = 10000
MAX_RETRIES = 10
RETRY_DELAY_S = 5


# =============================================================================
# Définition des tables Vox-SN
# =============================================================================
TABLES_DEFINITION: dict[bytes, dict[bytes, dict[str, Any]]] = {
    # -------------------------------------------------------------------------
    # Table : vox:posts
    # Row key : post_id (UUID inversé pour distribution uniforme)
    # -------------------------------------------------------------------------
    b"vox:posts": {
        b"meta": {"max_versions": 1, "compression": "GZ"},
        b"nlp": {"max_versions": 1, "compression": "GZ"},
        b"privacy": {"max_versions": 1, "compression": "GZ"},
    },

    # -------------------------------------------------------------------------
    # Table : vox:alertes (TTL 72h pour conformité RGPD)
    # Row key : timestamp_inv + service_cible (scan chronologique inverse)
    # -------------------------------------------------------------------------
    b"vox:alertes": {
        b"alerte": {
            "max_versions": 1,
            "time_to_live": 259200,   # 72h en secondes
            "compression": "GZ",
        },
    },

    # -------------------------------------------------------------------------
    # Table : vox:sentiment_agg (48h d'historique, 1 valeur / heure)
    # Row key : service_cible + timestamp_inv
    # -------------------------------------------------------------------------
    b"vox:sentiment_agg": {
        b"stats": {
            "max_versions": 48,       # 48h d'historique
            "compression": "GZ",
        },
    },
}


# =============================================================================
# Connexion robuste
# =============================================================================
def open_connection(retries: int = MAX_RETRIES) -> happybase.Connection:
    """Ouvre une connexion HBase avec retry."""
    for attempt in range(1, retries + 1):
        try:
            conn = happybase.Connection(
                host=HBASE_HOST,
                port=HBASE_PORT,
                timeout=HBASE_TIMEOUT_MS,
                autoconnect=True,
            )
            # Test rapide
            conn.tables()
            logger.info("Connexion HBase OK (%s:%d)", HBASE_HOST, HBASE_PORT)
            return conn
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Connexion HBase échouée (tentative %d/%d) : %s",
                attempt, retries, exc,
            )
            if attempt < retries:
                time.sleep(RETRY_DELAY_S)
    raise RuntimeError(
        f"Impossible de se connecter à HBase {HBASE_HOST}:{HBASE_PORT} "
        f"après {retries} tentatives."
    )


# =============================================================================
# Création des tables
# =============================================================================
def create_vox_tables() -> int:
    """
    Crée les tables Vox-SN si elles n'existent pas.

    Returns
    -------
    int
        Nombre de tables créées (0 si toutes existaient déjà).
    """
    conn = open_connection()
    try:
        existantes = [t.decode() for t in conn.tables()]
        logger.info("Tables existantes : %s", existantes)
        n_created = 0
        for table_name_b, families in TABLES_DEFINITION.items():
            table_name = table_name_b.decode()
            if table_name in existantes:
                logger.info("✓ Table %s déjà existante", table_name)
            else:
                conn.create_table(table_name_b, families)
                logger.info("✅ Table %s créée", table_name)
                n_created += 1
        return n_created
    finally:
        conn.close()


# =============================================================================
# Vérification du schéma (helper pour la soutenance)
# =============================================================================
def verify_schema() -> None:
    """Affiche le schéma de chaque table pour vérification."""
    conn = open_connection()
    try:
        for table_name_b in TABLES_DEFINITION:
            table_name = table_name_b.decode()
            try:
                table = conn.table(table_name)
                families = table.families()
                logger.info("[%s] families = %s", table_name, list(families.keys()))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Erreur lecture %s : %s", table_name, exc)
    finally:
        conn.close()


# =============================================================================
# Suppression (utilitaire de nettoyage)
# =============================================================================
def drop_vox_tables() -> None:
    """⚠️ DANGER : supprime toutes les tables vox:*. À utiliser avec précaution."""
    conn = open_connection()
    try:
        existantes = [t.decode() for t in conn.tables()]
        for table_name_b in TABLES_DEFINITION:
            table_name = table_name_b.decode()
            if table_name in existantes:
                try:
                    conn.disable_table(table_name_b)
                except Exception:  # noqa: BLE001
                    pass
                conn.delete_table(table_name_b)
                logger.warning("⛔ Table %s supprimée", table_name)
    finally:
        conn.close()


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "create"

    if action == "create":
        try:
            n = create_vox_tables()
            logger.info("Setup terminé : %d nouvelle(s) table(s) créée(s).", n)
            verify_schema()
        except Exception as exc:  # noqa: BLE001
            logger.error("Setup échoué : %s", exc)
            sys.exit(1)
    elif action == "verify":
        verify_schema()
    elif action == "drop":
        print("⚠️  Confirmation suppression tables (oui/non) : ", end="")
        if input().strip().lower() == "oui":
            drop_vox_tables()
        else:
            logger.info("Annulé.")
    else:
        logger.error("Action inconnue : %s. Usage : create | verify | drop", action)
        sys.exit(1)
