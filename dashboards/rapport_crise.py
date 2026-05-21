"""
rapport_crise.py — Livrable 4 — Rapport de crise Vox-SN
========================================================

Génère un rapport graphique en 4 panneaux illustrant une panne Wave :

    ┌──────────────────────────────┬──────────────────────────────┐
    │ 1. Timeline sentiment Wave   │ 2. Catégories de plaintes    │
    │    (résolution 15 min)       │    (barh)                    │
    ├──────────────────────────────┼──────────────────────────────┤
    │ 3. Battle Mobile Money       │ 4. Wordcloud Wolof/FR        │
    │    (sentiment par opérateur) │    (mots-clés plaintes)      │
    └──────────────────────────────┴──────────────────────────────┘

Sortie : rapport_crise_wave.png (300 DPI)

Mode dégradé : si Hive est inaccessible, utilise un dataset
de démonstration intégré (utile pour la soutenance offline).

Usage :
    python dashboards/rapport_crise.py
    python dashboards/rapport_crise.py --service ORANGE_MONEY --output rapport_om.png

Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Groupe Sonatel
Auteur    : Vox-SN Team - UADB M2 BD&IA 2025-2026
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # Backend sans display (utile en Docker)

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from wordcloud import WordCloud


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("RapportCrise")


# =============================================================================
# Configuration visuelle
# =============================================================================
COLORS = {
    "crise":   "#E53935",
    "warn":    "#FF7043",
    "neutral": "#5E35B1",
    "ok":      "#00695C",
    "info":    "#F57F17",
}

sns.set_style("whitegrid")
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "axes.titleweight": "bold",
    "axes.labelweight": "bold",
    "figure.titleweight": "bold",
})


# =============================================================================
# Source de données : Hive ou démo intégrée
# =============================================================================
def load_from_hive(service: str) -> pd.DataFrame:
    """Charge les données de crise depuis Hive."""
    from pyhive import hive

    conn = hive.Connection(host="hive-metastore", port=10000, database="vox_sn")
    sql = f"""
        SELECT ingestion_ts, service_cible, sentiment_score,
               categorie, texte_clean, langue, region
        FROM posts_analyses
        WHERE service_cible = '{service}'
          AND statut_alerte IN ('CRISE', 'NEGATIF_FORT')
          AND ingestion_ts >= DATE_SUB(CURRENT_TIMESTAMP, 1)
        ORDER BY ingestion_ts
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df


def load_battle_from_hive() -> pd.DataFrame:
    from pyhive import hive
    conn = hive.Connection(host="hive-metastore", port=10000, database="vox_sn")
    df = pd.read_sql("SELECT * FROM vue_battle_mobile_money", conn)
    conn.close()
    return df


def generate_demo_data(service: str = "WAVE") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Génère un dataset de démonstration réaliste (panne 4h sur Wave).
    Utilisé en mode dégradé pour la soutenance.
    """
    logger.info("Mode démo activé — génération de données simulées.")
    now = datetime.utcnow()
    start = now - timedelta(hours=4)

    # 300 posts sur 4h, score qui plonge puis remonte
    n = 300
    timestamps = pd.date_range(start, now, periods=n)

    # Courbe : descente brutale au t=1h, remontée progressive après t=3h
    elapsed_h = np.linspace(0, 4, n)
    score = np.where(
        elapsed_h < 1, -0.2,
        np.where(elapsed_h < 3, -0.7 - 0.1 * np.sin(elapsed_h * 2), -0.4 + 0.1 * elapsed_h)
    )
    score += np.random.normal(0, 0.1, n)
    score = np.clip(score, -1, 1)

    categories = np.random.choice(
        ["TECHNIQUE", "FRAUDE", "TARIF", "SERVICE_CLIENT", "AUTRE"],
        size=n, p=[0.55, 0.15, 0.12, 0.13, 0.05],
    )
    langues = np.random.choice(["FR", "WO", "EN"], size=n, p=[0.65, 0.30, 0.05])
    regions = np.random.choice(
        ["DAKAR", "THIES", "KAOLACK", "SAINT_LOUIS", "ZIGUINCHOR"],
        size=n,
    )

    sample_texts = {
        "FR": [
            "Wave bloqué impossible transfert",
            "Wave en panne argent perdu arnaque",
            "Wave indisponible depuis matin",
            "Wave transaction échouée scandaleux",
            "Wave bug application frais cachés",
        ],
        "WO": [
            "Wave dafa teye dafa neka",
            "Wave problem bi cher na",
            "Wave duma gënn xaalis bi",
        ],
        "EN": ["Wave is down again broken service"],
    }

    textes = [
        np.random.choice(sample_texts[lng])
        for lng in langues
    ]

    df = pd.DataFrame({
        "ingestion_ts": timestamps,
        "service_cible": service,
        "sentiment_score": score,
        "categorie": categories,
        "texte_clean": textes,
        "langue": langues,
        "region": regions,
    })

    # Battle Mobile Money
    battle = pd.DataFrame({
        "service_cible": ["WAVE", "ORANGE_MONEY", "FREE_MONEY"],
        "total_mentions": [350, 280, 220],
        "sentiment_moyen": [-0.55, -0.20, -0.15],
        "pct_positif": [25.0, 38.0, 42.0],
        "pct_critique": [42.0, 18.0, 12.0],
        "nb_fraudes": [12, 5, 3],
        "nb_pannes": [85, 24, 16],
    })

    return df, battle


def load_data(service: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Tente de charger depuis Hive ; bascule en démo si échec."""
    try:
        df = load_from_hive(service)
        battle = load_battle_from_hive()
        if df.empty:
            raise RuntimeError("Aucune donnée de crise dans Hive.")
        logger.info("Données Hive chargées : %d lignes", len(df))
        return df, battle
    except Exception as exc:  # noqa: BLE001
        logger.warning("Échec connexion Hive (%s) — bascule en mode démo.", exc)
        return generate_demo_data(service)


# =============================================================================
# Génération du rapport
# =============================================================================
def generate_report(
    df: pd.DataFrame,
    battle: pd.DataFrame,
    service: str,
    output: Path,
) -> None:
    """Construit la figure 4 panneaux."""

    df = df.copy()
    df["ingestion_ts"] = pd.to_datetime(df["ingestion_ts"])

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle(
        f"Rapport de Crise — Panne {service} Sénégal | Vox-SN",
        fontsize=18,
    )

    # -------------------------------------------------------------------------
    # Panneau 1 : Timeline sentiment (résolution 15 min)
    # -------------------------------------------------------------------------
    timeline = (
        df.set_index("ingestion_ts")["sentiment_score"]
        .resample("15min")
        .mean()
        .dropna()
    )
    ax1 = axes[0, 0]
    ax1.plot(timeline.index, timeline.values, color=COLORS["crise"], linewidth=2.5)
    ax1.axhline(y=-0.5, color="black", linestyle="--", label="Seuil crise (-0.5)")
    ax1.fill_between(
        timeline.index, timeline.values, -0.5,
        where=(timeline.values < -0.5),
        alpha=0.3, color=COLORS["crise"], label="Zone de crise",
    )
    ax1.set_title(f"Évolution sentiment {service} (résolution 15 min)")
    ax1.set_ylabel("Score sentiment")
    ax1.set_ylim(-1.05, 0.5)
    ax1.legend(loc="lower right")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax1.grid(alpha=0.3)

    # -------------------------------------------------------------------------
    # Panneau 2 : Catégories de plaintes (barh)
    # -------------------------------------------------------------------------
    cat_counts = df["categorie"].value_counts()
    ax2 = axes[0, 1]
    bar_colors = [COLORS["crise"], COLORS["warn"], COLORS["neutral"],
                  COLORS["ok"], COLORS["info"]]
    ax2.barh(
        cat_counts.index, cat_counts.values,
        color=bar_colors[: len(cat_counts)],
    )
    ax2.set_title("Catégories de plaintes durant la crise")
    ax2.set_xlabel("Nombre de posts")
    ax2.invert_yaxis()
    for i, v in enumerate(cat_counts.values):
        ax2.text(v + 2, i, str(v), va="center")

    # -------------------------------------------------------------------------
    # Panneau 3 : Battle Mobile Money
    # -------------------------------------------------------------------------
    ax3 = axes[1, 0]
    if not battle.empty:
        colors = [
            COLORS["crise"] if v < 0 else COLORS["ok"]
            for v in battle["sentiment_moyen"]
        ]
        bars = ax3.bar(
            battle["service_cible"], battle["sentiment_moyen"],
            color=colors, edgecolor="black",
        )
        ax3.axhline(y=0, color="black", linewidth=0.5)
        ax3.set_title("Battle Mobile Money — Sentiment moyen (7 jours)")
        ax3.set_ylabel("Score sentiment")
        ax3.set_ylim(-1, 1)
        # Annotation
        for bar, score in zip(bars, battle["sentiment_moyen"]):
            ax3.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + (0.05 if score >= 0 else -0.1),
                f"{score:.2f}",
                ha="center", fontweight="bold",
            )

    # -------------------------------------------------------------------------
    # Panneau 4 : Wordcloud
    # -------------------------------------------------------------------------
    ax4 = axes[1, 1]
    textes_negatifs = " ".join(df["texte_clean"].dropna().astype(str).tolist())
    if textes_negatifs.strip():
        wc = WordCloud(
            width=800,
            height=400,
            background_color="white",
            colormap="Reds",
            max_words=60,
            collocations=False,
        ).generate(textes_negatifs)
        ax4.imshow(wc, interpolation="bilinear")
    ax4.axis("off")
    ax4.set_title(f"Mots-clés plaintes {service} (Wolof + FR)")

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(output, dpi=200, bbox_inches="tight")
    plt.close()
    logger.info("✅ Rapport généré : %s", output)


# =============================================================================
# Métriques clés (sortie console)
# =============================================================================
def print_metrics(df: pd.DataFrame, service: str) -> None:
    print("\n" + "═" * 60)
    print(f"  MÉTRIQUES DE CRISE — {service}")
    print("═" * 60)
    print(f"  Posts négatifs                : {len(df):>6d}")
    print(f"  Score sentiment moyen         : {df['sentiment_score'].mean():>6.3f}")
    print(f"  % plaintes TECHNIQUE          : {(df['categorie']=='TECHNIQUE').mean()*100:>5.1f} %")
    print(f"  % plaintes FRAUDE             : {(df['categorie']=='FRAUDE').mean()*100:>5.1f} %")
    print(f"  Mentions Wolof                : {(df['langue']=='WO').sum():>6d}")
    print(f"  Mentions Français             : {(df['langue']=='FR').sum():>6d}")
    if not df.empty:
        debut = df["ingestion_ts"].min()
        fin = df["ingestion_ts"].max()
        print(f"  Durée de la crise             : {fin - debut}")
        print(f"  Régions touchées              : {df['region'].nunique()}")
    print("═" * 60 + "\n")


# =============================================================================
# CLI
# =============================================================================
def main() -> int:
    parser = argparse.ArgumentParser(description="Rapport de crise Vox-SN")
    parser.add_argument(
        "--service", default="WAVE",
        choices=["WAVE", "ORANGE_MONEY", "FREE_MONEY",
                 "SENELEC", "SEN_EAU", "TER"],
        help="Service à analyser.",
    )
    parser.add_argument(
        "--output", default="rapport_crise_wave.png",
        help="Fichier image de sortie.",
    )
    args = parser.parse_args()

    df, battle = load_data(args.service)
    if df.empty:
        logger.error("Aucune donnée à reporter.")
        return 1

    out = Path(args.output)
    generate_report(df, battle, args.service, out)
    print_metrics(df, args.service)
    return 0


if __name__ == "__main__":
    sys.exit(main())
