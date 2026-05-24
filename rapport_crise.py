# rapport_crise.py — Simulation d'une panne Mobile Money + génération rapport
# Livrable 4 : Rapport de crise scénario panne Wave Sénégal

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from wordcloud import WordCloud
from datetime import datetime, timedelta
import random

# ── 1. Simulation des données de crise Wave ──────────────────────────────
# Génère des données réalistes simulant une panne Wave de 3 heures
def simulate_crisis_data():
    """Simule une panne Wave avec posts négatifs sur 3 heures."""
    base_time = datetime(2025, 1, 15, 9, 0, 0)  # Panne à 9h00
    data = []
    textes = [
        ('Wave dafa teye, duma genn xaalis bi !', 'WO', 'TECHNIQUE'),
        ('Transfert Wave bloqué depuis 2h impossible support', 'FR', 'TECHNIQUE'),
        ('Transaction Wave échouée argent débité !', 'FR', 'FRAUDE'),
        ('Wave arnaque total argent perdu !', 'FR', 'FRAUDE'),
        ('Frais Wave trop cher na commission', 'FR', 'TARIF'),
        ('Wave indisponible ce matin inacceptable', 'FR', 'TECHNIQUE'),
        ('Support Wave ne répond jamais honte', 'FR', 'SERVICE_CLIENT'),
        ('Wave panne encore bug application', 'FR', 'TECHNIQUE'),
        ('Argent perdu Wave escroquerie !', 'FR', 'FRAUDE'),
        ('Wave dafa neka problem bi', 'WO', 'TECHNIQUE'),
    ]
    # Phase 1 : montée de la crise (9h-10h) — sentiment très négatif
    for i in range(40):
        texte, langue, cat = random.choice(textes)
        ts = base_time + timedelta(minutes=random.randint(0, 60))
        data.append({
            'ingestion_ts': ts,
            'service_cible': 'WAVE',
            'sentiment_score': random.uniform(-0.95, -0.6),
            'categorie': cat,
            'texte_clean': texte,
            'langue': langue,
            'region': random.choice(['DAKAR', 'THIES', 'KAOLACK']),
            'statut_alerte': 'CRISE'
        })
    # Phase 2 : pic de crise (10h-11h) — sentiment au plus bas
    for i in range(30):
        texte, langue, cat = random.choice(textes)
        ts = base_time + timedelta(minutes=random.randint(60, 120))
        data.append({
            'ingestion_ts': ts,
            'service_cible': 'WAVE',
            'sentiment_score': random.uniform(-1.0, -0.75),
            'categorie': cat,
            'texte_clean': texte,
            'langue': langue,
            'region': random.choice(['DAKAR', 'THIES', 'KAOLACK']),
            'statut_alerte': 'CRISE'
        })
    # Phase 3 : résolution progressive (11h-12h) — sentiment remonte
    for i in range(20):
        texte, langue, cat = random.choice(textes)
        ts = base_time + timedelta(minutes=random.randint(120, 180))
        data.append({
            'ingestion_ts': ts,
            'service_cible': 'WAVE',
            'sentiment_score': random.uniform(-0.6, -0.2),
            'categorie': cat,
            'texte_clean': texte,
            'langue': langue,
            'region': random.choice(['DAKAR', 'THIES', 'KAOLACK']),
            'statut_alerte': 'NEGATIF_FORT'
        })
    return pd.DataFrame(data)

def simulate_battle_data():
    """Simule la Battle Mobile Money durant la crise Wave."""
    return pd.DataFrame([
        {'service_cible': 'WAVE',         'sentiment_moyen': -0.72},
        {'service_cible': 'ORANGE_MONEY', 'sentiment_moyen':  0.15},
        {'service_cible': 'FREE_MONEY',   'sentiment_moyen': -0.05},
    ])

# ── 2. Chargement des données ─────────────────────────────────────────────
# Tentative de connexion Hive, fallback sur données simulées si indisponible
try:
    from pyhive import hive
    conn = hive.Connection(host='localhost', port=10000, database='vox_sn')
    crisis_df = pd.read_sql("""
        SELECT ingestion_ts, service_cible, sentiment_score,
               categorie, texte_clean, langue, region
        FROM posts_analyses
        WHERE service_cible = 'WAVE'
          AND statut_alerte IN ('CRISE','NEGATIF_FORT')
        ORDER BY ingestion_ts
    """, conn)
    battle_df = pd.read_sql("SELECT * FROM vue_battle_mobile_money", conn)
    # Si tables vides, utiliser données simulées
    if crisis_df.empty:
        print('[INFO] Tables Hive vides — utilisation des données simulées')
        crisis_df = simulate_crisis_data()
        battle_df = simulate_battle_data()
    else:
        crisis_df['ingestion_ts'] = pd.to_datetime(crisis_df['ingestion_ts'])
except Exception as e:
    print(f'[INFO] Hive non disponible ({e}) — utilisation des données simulées')
    crisis_df = simulate_crisis_data()
    battle_df  = simulate_battle_data()

# ── 3. Génération du rapport graphique en 4 panneaux ─────────────────────
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Rapport de Crise — Panne Wave Sénégal | Vox-SN',
             fontsize=16, fontweight='bold')

# Graphique 1 : Timeline du sentiment Wave (fenêtres 15min)
# Montre la chute du sentiment sous -0.5 pendant la panne
timeline = (crisis_df
            .set_index('ingestion_ts')['sentiment_score']
            .resample('15min').mean())
axes[0,0].plot(timeline.index, timeline.values, color='#E53935', linewidth=2)
axes[0,0].axhline(y=-0.5, color='black', linestyle='--', label='Seuil crise (-0.5)')
axes[0,0].fill_between(timeline.index, timeline.values, -0.5,
                        where=timeline.values < -0.5,
                        alpha=0.3, color='red')
axes[0,0].set_title('Évolution sentiment Wave (fenêtres 15min)')
axes[0,0].set_ylabel('Score sentiment')
axes[0,0].legend()
axes[0,0].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

# Graphique 2 : Distribution des catégories de plaintes durant la crise
# TECHNIQUE et FRAUDE dominent pendant une panne Mobile Money
cat_counts = crisis_df['categorie'].value_counts()
colors = ['#E53935','#FF7043','#5E35B1','#00695C','#F57F17']
axes[0,1].barh(cat_counts.index, cat_counts.values,
               color=colors[:len(cat_counts)])
axes[0,1].set_title('Catégories de plaintes durant la crise')
axes[0,1].set_xlabel('Nombre de posts')

# Graphique 3 : Battle Mobile Money — avantage concurrentiel durant la panne
# Orange Money et Free Money bénéficient de la panne Wave
if not battle_df.empty:
    x = range(len(battle_df))
    axes[1,0].bar(x, battle_df['sentiment_moyen'],
                  color=['#E53935' if v < 0 else '#00695C'
                         for v in battle_df['sentiment_moyen']])
    axes[1,0].set_xticks(list(x))
    axes[1,0].set_xticklabels(battle_df['service_cible'])
    axes[1,0].set_title('Battle Mobile Money — Score sentiment moyen')
    axes[1,0].set_ylabel('Score sentiment')
    axes[1,0].axhline(y=0, color='black', linewidth=0.5)

# Graphique 4 : Nuage de mots des plaintes Wave (Wolof + Français)
textes_negatifs = ' '.join(crisis_df['texte_clean'].dropna().tolist())
if textes_negatifs.strip():
    wc = WordCloud(width=600, height=300, background_color='white',
                   colormap='Reds', max_words=50).generate(textes_negatifs)
    axes[1,1].imshow(wc, interpolation='bilinear')
    axes[1,1].axis('off')
    axes[1,1].set_title('Mots-clés des plaintes Wave (Wolof + FR)')

plt.tight_layout()
plt.savefig('rapport_crise_wave.png', dpi=150, bbox_inches='tight')
print('Rapport de crise généré : rapport_crise_wave.png')

# ── 4. Métriques clés de la crise ────────────────────────────────────────
print('\n=== MÉTRIQUES DE CRISE ===')
print(f'Total posts négatifs    : {len(crisis_df)}')
print(f'Score sentiment moyen   : {crisis_df["sentiment_score"].mean():.3f}')
print(f'% plaintes Fraude       : {(crisis_df["categorie"]=="FRAUDE").mean()*100:.1f}%')
print(f'% plaintes Technique    : {(crisis_df["categorie"]=="TECHNIQUE").mean()*100:.1f}%')
if not crisis_df.empty:
    debut  = crisis_df['ingestion_ts'].min()
    fin    = crisis_df['ingestion_ts'].max()
    duree  = fin - debut
    print(f'Début de la crise       : {debut}')
    print(f'Fin de la crise         : {fin}')
    print(f'Durée de la crise       : {duree}')
    print(f'Langues                 : {crisis_df["langue"].value_counts().to_dict()}')
    print(f'Régions touchées        : {crisis_df["region"].value_counts().to_dict()}')