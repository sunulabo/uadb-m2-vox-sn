"""
schema.py — Contrats Pandera pour Vox-SN
=========================================

Définit le schéma de validation strict appliqué aux posts citoyens AVANT
leur entrée dans le pipeline NLP. Inclut une détection active des PII
(numéros sénégalais, IDs de transaction Mobile Money) basée sur regex.

Tout post contenant un PII brut dans le champ texte est REJETÉ.

Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
Auteur    : Vox-SN Team - UADB M2 BD&IA 2025-2026
"""

from __future__ import annotations

import logging
import re
from typing import Final

import pandas as pd
import pandera as pa
from pandera.typing import Series

# -----------------------------------------------------------------------------
# Logger configuré pour ce module
# -----------------------------------------------------------------------------
logger = logging.getLogger("VoxSchema")


# -----------------------------------------------------------------------------
# Patterns regex de détection des données sensibles (PII)
# -----------------------------------------------------------------------------
# Spécificités Sénégal :
#   - Format international : +221 suivi de 9 chiffres
#   - Mobile local         : 7X (Orange) ou 70/76/77/78 etc.
#   - Numéros de compte    : 10 à 16 chiffres consécutifs (transactions MM)
PII_PATTERNS: Final[list[str]] = [
    r"\+?221[0-9]{9}",      # Numéro sénégalais international
    r"\b7[0-9]{8}\b",       # Mobile local (commence par 7)
    r"\b[0-9]{10,16}\b",    # Numéro de compte / transaction Mobile Money
]

# Pré-compilation pour performance
_PII_REGEX = [re.compile(p) for p in PII_PATTERNS]


def contains_pii(text: str) -> bool:
    """
    Détermine si le texte contient un PII brut.

    Parameters
    ----------
    text : str
        Texte du post citoyen.

    Returns
    -------
    bool
        True si au moins un pattern PII matche.
    """
    if not isinstance(text, str) or not text:
        return False
    return any(p.search(text) for p in _PII_REGEX)


def redact_pii(text: str, placeholder: str = "[PII_REDACTED]") -> str:
    """
    Remplace les PII détectés par un placeholder.

    Utile en cas de besoin de conserver le texte avec PII masqués.

    Parameters
    ----------
    text : str
        Texte d'origine.
    placeholder : str
        Remplaçant des PII.

    Returns
    -------
    str
        Texte caviardé.
    """
    if not text:
        return ""
    redacted = text
    for pattern in _PII_REGEX:
        redacted = pattern.sub(placeholder, redacted)
    return redacted


# -----------------------------------------------------------------------------
# Schéma Pandera : posts bruts (entrée du pipeline)
# -----------------------------------------------------------------------------
class SocialSentimentSchema(pa.SchemaModel):
    """
    Schéma strict pour le topic Kafka `social_raw`.

    Tous les champs sont obligatoires. La validation s'effectue en mode
    `lazy=True` afin de collecter toutes les violations au lieu d'arrêter
    à la première.
    """

    post_id: Series[str] = pa.Field(unique=True, description="UUID du post")

    service_cible: Series[str] = pa.Field(
        isin=[
            "SENELEC", "SEN_EAU", "TER",
            "WAVE", "ORANGE_MONEY", "FREE_MONEY",
        ],
        description="Opérateur ou service public visé par le post",
    )

    texte_du_post: Series[str] = pa.Field(
        str_length={"min_value": 5, "max_value": 280},
        description="Contenu du post (longueur Twitter-like)",
    )

    langue: Series[str] = pa.Field(
        isin=["FR", "WO", "EN"],
        description="Code ISO 639-1 simplifié (FR/WO=Wolof/EN)",
    )

    timestamp: Series[str] = pa.Field(
        str_matches=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$",
        description="Timestamp ISO 8601 UTC",
    )

    canal: Series[str] = pa.Field(
        isin=["TWITTER", "FACEBOOK", "WHATSAPP", "RECLAMATION"],
        description="Canal de collecte (NiFi route en fonction)",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Check personnalisé : aucun PII brut ne doit entrer dans le pipeline
    # ─────────────────────────────────────────────────────────────────────────
    @pa.check("texte_du_post", name="no_raw_phone_in_text")
    def check_no_pii(cls, series: pd.Series) -> pd.Series:  # noqa: N805
        """
        Vérifie l'absence de PII bruts dans le texte.

        Returns
        -------
        pd.Series[bool]
            True pour les lignes valides (sans PII), False pour celles
            contenant un PII — qui seront rejetées par Pandera.
        """
        flagged = series.apply(contains_pii)
        if flagged.any():
            logger.warning(
                "%d post(s) contiennent des PII détectés et seront rejetés",
                int(flagged.sum()),
            )
        return ~flagged

    class Config:
        strict = True
        coerce = True


# -----------------------------------------------------------------------------
# Schéma Pandera : posts analysés (sortie du pipeline NLP)
# -----------------------------------------------------------------------------
class AnalyzedPostSchema(pa.SchemaModel):
    """
    Schéma de sortie après passage par le pipeline NLP Spark.

    Garantit qu'aucun champ PII brut (user_id, phone_number) ne soit
    présent en aval — seul `citizen_id_secure` (hash SHA-256) subsiste.
    """

    post_id: Series[str] = pa.Field(unique=True)
    citizen_id_secure: Series[str] = pa.Field(
        str_matches=r"^[0-9a-f]{64}$",
        description="SHA-256 du user_id (jamais brut)",
    )
    service_cible: Series[str]
    texte_clean: Series[str] = pa.Field(nullable=True)
    langue: Series[str] = pa.Field(isin=["FR", "WO", "EN"])
    sentiment_score: Series[float] = pa.Field(in_range={"min_value": -1.0, "max_value": 1.0})
    sentiment_label: Series[str] = pa.Field(
        isin=["NEGATIF_FORT", "NEGATIF", "NEUTRE", "POSITIF"]
    )
    categorie: Series[str] = pa.Field(
        isin=["TARIF", "TECHNIQUE", "FRAUDE", "SERVICE_CLIENT", "POSITIF", "AUTRE", "INCONNU"]
    )
    statut_alerte: Series[str] = pa.Field(
        isin=["CRISE", "NEGATIF_FORT", "NORMAL"]
    )

    class Config:
        strict = False  # peut contenir des colonnes additionnelles (timestamp etc.)
        coerce = True


# -----------------------------------------------------------------------------
# Helpers de validation
# -----------------------------------------------------------------------------
def validate_and_filter(
    df: pd.DataFrame,
    schema_class: type[pa.SchemaModel],
) -> pd.DataFrame:
    """
    Valide un DataFrame contre un schéma Pandera et retourne uniquement
    les lignes valides.

    Les lignes en erreur sont logguées mais ne lèvent pas d'exception
    (mode best-effort, important pour un streaming temps réel).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame en entrée.
    schema_class : type[pa.SchemaModel]
        Classe schéma Pandera (ex: SocialSentimentSchema).

    Returns
    -------
    pd.DataFrame
        DataFrame nettoyé contenant uniquement les lignes valides.
    """
    if df.empty:
        return df
    try:
        validated = schema_class.validate(df, lazy=True)
        return validated
    except pa.errors.SchemaErrors as exc:
        err = exc.failure_cases
        logger.warning(
            "[%s] %d ligne(s) rejetée(s) lors de la validation",
            schema_class.__name__,
            len(err),
        )
        # On extrait les indices à supprimer
        invalid_idx = err["index"].dropna().astype(int).unique()
        valid_idx = df.index.difference(invalid_idx)
        return df.loc[valid_idx]


# -----------------------------------------------------------------------------
# Auto-test (lancer : `python schema.py`)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    # Cas 1 : post valide
    sample_ok = pd.DataFrame([{
        "post_id": "uuid-001",
        "service_cible": "WAVE",
        "texte_du_post": "Wave dafa baax, j'aime bien",
        "langue": "FR",
        "timestamp": "2025-11-01T12:30:00Z",
        "canal": "TWITTER",
    }])
    print("\n--- Cas 1 : post valide ---")
    print(validate_and_filter(sample_ok, SocialSentimentSchema))

    # Cas 2 : post avec PII (numéro 771234567)
    sample_pii = pd.DataFrame([{
        "post_id": "uuid-002",
        "service_cible": "WAVE",
        "texte_du_post": "Mon numéro 771234567 a été bloqué !",
        "langue": "FR",
        "timestamp": "2025-11-01T12:30:00Z",
        "canal": "TWITTER",
    }])
    print("\n--- Cas 2 : post contenant un PII ---")
    print(f"Détection PII : {contains_pii(sample_pii['texte_du_post'].iloc[0])}")
    print(f"Redaction     : {redact_pii(sample_pii['texte_du_post'].iloc[0])}")
    filtered = validate_and_filter(sample_pii, SocialSentimentSchema)
    print(f"Après filtrage : {len(filtered)} ligne(s) restante(s)")
