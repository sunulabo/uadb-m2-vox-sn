# -*- coding: utf-8 -*-
"""
================================================================================
 Vox-SN | Dashboard Interactif Battle Mobile Money
--------------------------------------------------------------------------------
 UADB | Master 2 Big Data & IA | 2025-2026
 Encadrant : Mr Ahmed Ben Sidy Bouya SEYE - Senior Big Data & AI Engineer
                                              Groupe Sonatel
================================================================================

OBJECTIF
--------
Génère un dashboard HTML INTERACTIF (Plotly) comparant en temps réel les trois
acteurs Mobile Money du Sénégal : WAVE, ORANGE_MONEY, FREE_MONEY.

PANNEAUX
--------
1. Gauge sentiment moyen par opérateur (KPI cards)
2. Évolution temporelle du sentiment (lignes 7 jours glissants)
3. Distribution des catégories de plaintes (stacked bar)
4. Heatmap régionale × opérateur (intensité du mécontentement)
5. Top 10 régions les plus négatives
6. Treemap parts de voix

USAGE
-----
    python dashboard_battle_mm.py --output battle_mm.html
    python dashboard_battle_mm.py --demo  # données simulées si Hive inaccessible

NOTE
----
Mode dégradé : si la connexion Hive échoue, des données démo sont générées
afin que le dashboard reste utilisable pour la démonstration en soutenance.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import plotly.express as px
except ImportError:
    print("ERREUR : plotly non installé. Installez avec : pip install plotly")
    sys.exit(1)

# ─── Configuration logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s :: %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('BattleMM')


# ─── Constantes projet ────────────────────────────────────────────────────────
OPERATEURS_MM = ['WAVE', 'ORANGE_MONEY', 'FREE_MONEY']
COULEURS = {
    'WAVE':         '#1CB0F6',  # Bleu Wave
    'ORANGE_MONEY': '#FF7900',  # Orange Money
    'FREE_MONEY':   '#CD0F47',  # Rouge Free
}
REGIONS_SN = ['DAKAR', 'THIES', 'KAOLACK', 'SAINT_LOUIS', 'ZIGUINCHOR',
              'DIOURBEL', 'FATICK', 'KOLDA', 'LOUGA', 'MATAM']
CATEGORIES = ['TARIF', 'TECHNIQUE', 'FRAUDE', 'SERVICE_CLIENT', 'POSITIF', 'AUTRE']


# ─── Chargement des données ───────────────────────────────────────────────────
def load_from_hive() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Charge les données analysées depuis Hive.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (df_posts, df_battle) : posts détaillés + agrégation Battle MM

    Raises
    ------
    ConnectionError
        Si Hive Metastore n'est pas joignable.
    """
    try:
        from pyhive import hive
    except ImportError as e:
        raise ConnectionError(f'pyhive non installé : {e}') from e

    logger.info('Connexion à Hive Metastore (hive-metastore:10000)...')
    conn = hive.Connection(host='hive-metastore', port=10000, database='vox_sn')

    posts_sql = """
        SELECT post_id, service_cible, sentiment_score, sentiment_label,
               categorie, region, langue, ingestion_ts
        FROM posts_analyses
        WHERE service_cible IN ('WAVE','ORANGE_MONEY','FREE_MONEY')
          AND date_post >= DATE_SUB(CURRENT_DATE(), 7)
    """
    df_posts = pd.read_sql(posts_sql, conn)
    df_battle = pd.read_sql('SELECT * FROM vue_battle_mobile_money', conn)
    logger.info(f'{len(df_posts)} posts chargés depuis Hive')
    return df_posts, df_battle


def generate_demo_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Génère des données simulées réalistes pour la démo soutenance.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        DataFrames identiques à ceux produits par load_from_hive().
    """
    logger.warning('Mode dégradé : génération de données simulées')
    np.random.seed(42)
    rows = []
    now = datetime.utcnow()

    # Calibrage du sentiment moyen par opérateur (réalité terrain)
    profil_sentiment = {
        'WAVE':         (+0.15, 0.45),  # moyenne, écart-type
        'ORANGE_MONEY': (-0.10, 0.50),
        'FREE_MONEY':   (-0.25, 0.55),
    }

    for service in OPERATEURS_MM:
        mu, sigma = profil_sentiment[service]
        nb_posts = np.random.randint(800, 1200)
        for _ in range(nb_posts):
            score = float(np.clip(np.random.normal(mu, sigma), -1.0, 1.0))
            if score < -0.5:
                label = 'NEGATIF_FORT'
            elif score < 0.0:
                label = 'NEGATIF'
            elif score > 0.3:
                label = 'POSITIF'
            else:
                label = 'NEUTRE'
            if score < -0.5:
                cat = np.random.choice(['TECHNIQUE', 'FRAUDE', 'TARIF'],
                                       p=[0.55, 0.25, 0.20])
            elif score > 0.3:
                cat = 'POSITIF'
            else:
                cat = np.random.choice(['SERVICE_CLIENT', 'TARIF', 'AUTRE'],
                                       p=[0.45, 0.35, 0.20])

            rows.append({
                'post_id': f'demo_{len(rows):06d}',
                'service_cible': service,
                'sentiment_score': score,
                'sentiment_label': label,
                'categorie': cat,
                'region': np.random.choice(REGIONS_SN,
                                           p=[0.35, 0.13, 0.10, 0.09, 0.07,
                                              0.07, 0.06, 0.05, 0.04, 0.04]),
                'langue': np.random.choice(['FR', 'WO', 'EN'], p=[0.6, 0.35, 0.05]),
                'ingestion_ts': now - timedelta(hours=np.random.uniform(0, 168)),
            })
    df_posts = pd.DataFrame(rows)
    df_battle = (df_posts.groupby('service_cible')
                 .agg(total_mentions=('post_id', 'count'),
                      sentiment_moyen=('sentiment_score', 'mean'),
                      pct_positif=('sentiment_label',
                                   lambda s: (s == 'POSITIF').mean() * 100),
                      pct_critique=('sentiment_label',
                                    lambda s: (s == 'NEGATIF_FORT').mean() * 100),
                      nb_fraudes=('categorie',
                                  lambda s: (s == 'FRAUDE').sum()),
                      nb_pannes=('categorie',
                                 lambda s: (s == 'TECHNIQUE').sum()))
                 .reset_index())
    return df_posts, df_battle


# ─── Construction des graphiques ──────────────────────────────────────────────
def build_dashboard(df_posts: pd.DataFrame, df_battle: pd.DataFrame) -> go.Figure:
    """
    Construit le dashboard Plotly multi-panneaux.

    Parameters
    ----------
    df_posts : pd.DataFrame
        Détail des posts (1 ligne par post).
    df_battle : pd.DataFrame
        Agrégations par opérateur.

    Returns
    -------
    go.Figure
        Figure Plotly prête à être exportée en HTML interactif.
    """
    fig = make_subplots(
        rows=3, cols=2,
        specs=[
            [{'type': 'indicator'}, {'type': 'indicator'}],
            [{'type': 'scatter', 'colspan': 2}, None],
            [{'type': 'bar'}, {'type': 'heatmap'}],
        ],
        row_heights=[0.20, 0.40, 0.40],
        subplot_titles=(
            'Sentiment moyen — WAVE',
            'Sentiment moyen — ORANGE_MONEY / FREE_MONEY',
            'Évolution temporelle du sentiment (7 jours)',
            'Catégories de plaintes par opérateur',
            'Heatmap régions × opérateurs (négativité)',
        ),
        vertical_spacing=0.10,
    )

    # ── 1. Indicateurs jauges (KPI) ──
    for idx, service in enumerate(OPERATEURS_MM):
        row_data = df_battle[df_battle['service_cible'] == service]
        if row_data.empty:
            continue
        score = float(row_data['sentiment_moyen'].iloc[0])
        if idx == 0:
            row, col = 1, 1
        else:
            row, col = 1, 2
        fig.add_trace(
            go.Indicator(
                mode='gauge+number+delta',
                value=score,
                domain={'row': 0, 'column': 0},
                title={'text': f'<b>{service}</b>', 'font': {'size': 14}},
                delta={'reference': 0, 'increasing': {'color': '#00C853'},
                       'decreasing': {'color': '#D50000'}},
                gauge={
                    'axis': {'range': [-1, 1]},
                    'bar': {'color': COULEURS.get(service, '#777')},
                    'steps': [
                        {'range': [-1.0, -0.5], 'color': '#FFCDD2'},
                        {'range': [-0.5,  0.0], 'color': '#FFF59D'},
                        {'range': [ 0.0,  1.0], 'color': '#C8E6C9'},
                    ],
                    'threshold': {
                        'line': {'color': 'black', 'width': 3},
                        'thickness': 0.75,
                        'value': -0.5,
                    },
                },
                number={'valueformat': '.3f'},
            ),
            row=row, col=col,
        )

    # ── 2. Timeline sentiment 7 jours ──
    df_posts['ingestion_ts'] = pd.to_datetime(df_posts['ingestion_ts'])
    df_time = (df_posts
               .set_index('ingestion_ts')
               .groupby('service_cible')['sentiment_score']
               .resample('3h')
               .mean()
               .reset_index())
    for service in OPERATEURS_MM:
        sub = df_time[df_time['service_cible'] == service]
        if sub.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=sub['ingestion_ts'], y=sub['sentiment_score'],
                mode='lines+markers',
                name=service,
                line={'color': COULEURS[service], 'width': 2.5},
                marker={'size': 6},
                hovertemplate=f'<b>{service}</b><br>'
                              + '%{x|%d %b %H:%M}<br>'
                              + 'Sentiment : %{y:.3f}<extra></extra>',
            ),
            row=2, col=1,
        )
    fig.add_hline(y=-0.5, line_dash='dash', line_color='red',
                  annotation_text='Seuil crise', row=2, col=1)

    # ── 3. Catégories par opérateur ──
    df_cat = (df_posts.groupby(['service_cible', 'categorie'])
              .size().reset_index(name='nb'))
    for cat in CATEGORIES:
        sub = df_cat[df_cat['categorie'] == cat]
        if sub.empty:
            continue
        fig.add_trace(
            go.Bar(
                x=sub['service_cible'], y=sub['nb'],
                name=cat,
                hovertemplate='<b>%{x}</b><br>'
                              + f'{cat} : ' + '%{y}<extra></extra>',
            ),
            row=3, col=1,
        )

    # ── 4. Heatmap régions × opérateurs (% négatif fort) ──
    df_neg = (df_posts[df_posts['sentiment_label'] == 'NEGATIF_FORT']
              .groupby(['region', 'service_cible']).size()
              .unstack(fill_value=0))
    df_total = df_posts.groupby(['region', 'service_cible']).size().unstack(fill_value=0)
    df_pct = (df_neg / df_total.replace(0, np.nan) * 100).fillna(0)
    df_pct = df_pct.reindex(REGIONS_SN, fill_value=0)
    df_pct = df_pct.reindex(columns=OPERATEURS_MM, fill_value=0)

    fig.add_trace(
        go.Heatmap(
            z=df_pct.values, x=df_pct.columns, y=df_pct.index,
            colorscale='Reds', colorbar={'title': '% négatif'},
            hovertemplate='Région : %{y}<br>'
                          + 'Opérateur : %{x}<br>'
                          + '%{z:.1f}% négatif<extra></extra>',
        ),
        row=3, col=2,
    )

    # ── Mise en forme globale ──
    fig.update_layout(
        title={
            'text': '<b>Vox-SN | Battle Mobile Money — Sénégal</b><br>'
                    + '<sup>Dashboard temps réel | UADB Master 2 Big Data & IA '
                    + '| 2025-2026</sup>',
            'x': 0.5, 'xanchor': 'center',
        },
        height=1100,
        showlegend=True,
        barmode='stack',
        template='plotly_white',
        font={'family': 'Helvetica, Arial', 'size': 12},
        margin={'l': 60, 'r': 60, 't': 100, 'b': 50},
    )
    return fig


# ─── Point d'entrée ───────────────────────────────────────────────────────────
def main() -> None:
    """CLI : génère le dashboard HTML et affiche les métriques clés."""
    parser = argparse.ArgumentParser(description='Dashboard Battle Mobile Money')
    parser.add_argument('--output', default='battle_mm.html',
                        help='Chemin du fichier HTML de sortie')
    parser.add_argument('--demo', action='store_true',
                        help='Forcer le mode données simulées')
    args = parser.parse_args()

    if args.demo:
        df_posts, df_battle = generate_demo_data()
    else:
        try:
            df_posts, df_battle = load_from_hive()
        except (ConnectionError, Exception) as e:
            logger.warning(f'Hive indisponible ({e}) → bascule mode démo')
            df_posts, df_battle = generate_demo_data()

    logger.info('Construction du dashboard...')
    fig = build_dashboard(df_posts, df_battle)

    output_path = Path(args.output).resolve()
    fig.write_html(str(output_path), include_plotlyjs='cdn', full_html=True)
    logger.info(f'Dashboard généré : {output_path}')

    # Métriques clés
    print('\n' + '=' * 65)
    print('METRIQUES BATTLE MOBILE MONEY')
    print('=' * 65)
    for _, row in df_battle.iterrows():
        print(f"  {row['service_cible']:<14} "
              f"sentiment={row['sentiment_moyen']:+.3f}  "
              f"mentions={int(row['total_mentions']):>5}  "
              f"%critique={row.get('pct_critique', 0):.1f}%")
    print('=' * 65)


if __name__ == '__main__':
    main()
