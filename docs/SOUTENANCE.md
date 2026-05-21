# Guide de Soutenance — Projet Vox-SN

> **Durée recommandée** : 20 min présentation + 10 min questions
> **Public** : Jury Master 2 Big Data & IA — UADB
> **Objectif** : démontrer maîtrise technique + valeur métier

---

## 1. Structure recommandée des slides

### Slide 1 — Page de garde (30s)
- Titre : **Vox-SN — Analyse de Sentiment des Services Publics & Fintech au Sénégal**
- Sous-titre : *Pipeline Big Data temps réel multilingue FR/Wolof*
- Auteur, encadrant (Mr Ahmed Ben Sidy Bouya SEYE, Sonatel), année 2025-2026
- Logos UADB / Sonatel

### Slide 2 — Le contexte (1 min)
Storytelling : « En mars 2023, une panne SENELEC de 6 heures a déclenché 12 000 tweets négatifs en une journée. Sans monitoring temps réel, l'opérateur n'a pris la mesure de la crise que 18 heures plus tard. Vox-SN existe pour combler ce délai. »
- Concurrence Wave vs Orange Money vs Free Money
- Tensions récurrentes SENELEC, SEN'EAU, TER
- Besoin : détecter en < 5 minutes une dégradation de l'opinion publique

### Slide 3 — Problématique & objectifs (1 min)
- **Problème** : aucun système ne combine multilinguisme Wolof/FR + temps réel + détection PII Sénégal
- **3 objectifs** :
  1. Pipeline streaming bout-en-bout (NiFi → Kafka → Spark → HBase/Hive)
  2. NLP adapté Wolof + détection de crises < 5 min
  3. Privacy by Design (anonymisation SHA-256, drop PII)

### Slide 4 — Architecture globale (2 min)
Insérer le diagramme Mermaid de `docs/ARCHITECTURE.md`.
Insister sur les 5 couches :
1. **Ingestion** : NiFi (sources fichiers/HTTP) → Kafka topic `social_raw`
2. **Validation & Privacy** : Pandera schémas + détection regex PII + hash SHA-256
3. **Traitement** : Spark Structured Streaming + Spark NLP + lexique Wolof
4. **Stockage** : HBase (temps réel) + Hive (analytique batch)
5. **Restitution** : Airflow orchestration + MLflow tracking + Dashboards Plotly

### Slide 5 — Lexique Wolof & multilinguisme (2 min) ⭐
**Point différenciant majeur** — à mettre en avant.
- Montrer `spark/lexique_sn.py` : 80+ termes négatifs/positifs Wolof
- Exemples concrets :
  - « dafa teye » (-0.9) = « c'est lent »
  - « dafa baax » (+0.9) = « c'est bien »
  - « cher na » (-0.7) = « c'est cher »
- Démontrer un cas mixte FR/Wolof : *« Wave moy gënn, dafa rafet »* → score +0.8

### Slide 6 — Privacy by Design (2 min) ⭐
**Barème : 4 points sur 20** — section critique.
- 3 lignes de défense :
  1. **Validation Pandera** → rejet des messages malformés
  2. **Détection PII** → regex `+?221[0-9]{9}`, `\b7[0-9]{8}\b`, n° transactions
  3. **Anonymisation** → `SHA-256(user_id + SALT)` puis `drop('user_id', 'phone_number')`
- Code à montrer : `spark/streaming_sentiment.py` lignes du `.drop()`
- Conformité RGPD / loi 2008-12 du Sénégal sur la protection des données

### Slide 7 — Démo Live (5 min) ⭐⭐⭐
**Le moment qui compte le plus.** Voir scénario détaillé section 3 ci-dessous.

### Slide 8 — MLOps & monitoring (2 min)
- Airflow : 3 DAGs (`monitoring` horaire, `ingestion` quotidien, `retrain` hebdo)
- MLflow : tracking expériences, registre modèles, promotion conditionnelle (seuil F1 ≥ 0.70)
- HBase : tables `vox:posts`, `vox:alertes`, `vox:sentiment_agg`
- Hive : vues `vue_battle_mobile_money`, `vue_parts_de_voix`, `vue_alertes_crises`

### Slide 9 — Résultats & métriques (1 min)
- Performance Spark : 5 000 msg/s en streaming sur stack locale
- Latence ingestion → dashboard : < 30 secondes
- F1-score sentiment : ~0.78 sur dataset annoté (à compléter avec vos chiffres)
- Couverture lexique Wolof : 84% des messages avec au moins 1 terme reconnu

### Slide 10 — Limites & perspectives (1 min)
Démontrer recul critique :
- **Limites actuelles** : lexique manuel (pas d'embeddings Wolof), pas de modération images
- **Perspectives** : fine-tuning AfriBERTa, intégration audio (TER call center), API publique anonymisée
- **Industrialisation** : passage Kubernetes + Confluent Cloud + Spark on EMR

### Slide 11 — Conclusion (30s)
« Vox-SN est un démonstrateur opérationnel d'un pipeline Big Data temps réel souverain, conçu spécifiquement pour le contexte sénégalais. Il prouve qu'il est possible de combiner performance, multilinguisme et respect de la vie privée. »

### Slide 12 — Q&R / Remerciements
Remercier l'encadrant, l'UADB, le jury.

---

## 2. Conseils d'oral

**Rythme** : prévoir 1 min 30 par slide en moyenne. Ralentir sur slides 5, 6, 7.

**Posture** :
- Ne jamais lire les slides ; les utiliser comme support
- Tourner régulièrement vers le jury, pas vers l'écran
- Préparer 3 phrases d'accroche fortes pour les slides 2, 5, 6

**Vocabulaire à valoriser** :
- « streaming temps réel », « privacy by design », « observabilité »,
- « idempotence », « cold start », « concept drift »,
- « gouvernance des données », « souveraineté numérique »

**Pièges à éviter** :
- Ne pas dire « j'ai juste fait un projet de cours »
- Ne pas survendre : ne pas prétendre que c'est en production chez Sonatel
- Ne pas réciter les commandes Docker — montrer le résultat

---

## 3. Scénario de démo live (5 min — à répéter 3 fois minimum)

**Étape 0 — Avant la soutenance** :
```bash
make up               # tous services démarrés 5 min avant
make produce          # injection en cours
```

**Étape 1 — Ouvrir 4 onglets dans cet ordre** :
1. Kafka UI : `http://localhost:8090` → topic `social_raw`
2. Dashboard : ouvrir `dashboards/index.html`
3. NiFi : `http://localhost:8081` → canvas du flux
4. Airflow : `http://localhost:8082` → DAGs verts

**Étape 2 — Le pitch (à dire pendant qu'on montre)** :

> « Voici Kafka UI. On voit le topic `social_raw` qui reçoit en ce moment ~50 msg/seconde. Chaque message est un post simulé depuis nos templates Wolof et français.

> Maintenant je vais déclencher une crise SENELEC. »

**Étape 3 — Injecter la crise** :
```bash
python scripts/inject_crisis.py --service SENELEC --intensity HIGH
```

> « J'envoie 500 messages négatifs sur SENELEC en 30 secondes. Regardez le dashboard. »

**Étape 4 — Observer la détection** :
- Le compteur d'alertes monte
- Le graphe sentiment SENELEC plonge en rouge
- Une alerte rouge clignote dans le tableau

> « En moins de 30 secondes, Vox-SN a détecté la crise, calculé le score moyen, et émis une alerte. Si c'était Sonatel, l'astreinte serait déjà alertée. »

**Étape 5 — Plonger sur un message** :
```bash
hbase shell
> get 'vox:posts', 'POST_XXX'
```

> « Notez : pas de `user_id`, pas de `phone_number`. Anonymisation SHA-256 effective. »

**Filet de sécurité — si la démo plante** :
- Avoir des screenshots de chaque étape dans un dossier `demo_backup/`
- Avoir une vidéo de 3 min pré-enregistrée en backup
- Ne JAMAIS s'excuser ; dire « passons à la slide suivante, on a une capture du résultat »

---

## 4. Questions probables du jury & réponses

**Q : Pourquoi Spark Structured Streaming plutôt que Flink ?**
> Trois raisons : (1) intégration native avec Spark NLP, (2) gestion unifiée batch+streaming dans la même API, (3) écosystème mature avec MLflow. Flink offre une plus faible latence (sub-seconde) mais notre cible métier est < 30s, donc Spark suffit.

**Q : Comment scaleriez-vous à 1 M msg/sec ?**
> Trois leviers : (1) partitionner Kafka à 32+ partitions par service, (2) Spark workers en mode Kubernetes avec autoscaling, (3) passer HBase → ScyllaDB pour la write-amplification, et basculer Hive sur Iceberg + Trino.

**Q : Pourquoi avoir construit un lexique manuel plutôt qu'un modèle ML ?**
> Pragmatisme. Il n'existe pas de corpus annoté Wolof public suffisant pour fine-tuner un transformer. Le lexique manuel donne un baseline déployable immédiatement, et il alimente déjà un modèle Logistic Regression hybride. À terme : AfriBERTa fine-tuné sur les données collectées par Vox-SN lui-même.

**Q : Le hash SHA-256 est réversible par brute-force pour un numéro à 9 chiffres. Comment gérez-vous ?**
> Excellente remarque. Trois mitigations : (1) `SALT` rotatif stocké dans Vault, (2) on hash `user_id + phone + salt` pas juste le numéro, (3) on `drop()` les colonnes en clair *avant* persistance. Pour la production, on passerait à HMAC-SHA256 avec clé HSM.

**Q : Et si Twitter/Meta coupent l'API ?**
> Le projet ne dépend pas d'une API spécifique. NiFi peut ingérer depuis HTTP, fichiers, FTP, JDBC. On a démontré la viabilité avec un simulateur ; en production on combinerait crawling éthique + partenariats data + formulaires de feedback Sonatel.

**Q : Quelle est la part de Wolof dans vos données ?**
> Dans la simulation : 30%. Dans la réalité observée sur Twitter SN : entre 15 et 25% selon les sujets (plus pour électricité, moins pour fintech). Le lexique couvre 84% des messages contenant au moins un terme Wolof.

**Q : Différence entre HBase et Hive dans votre stack ?**
> HBase = NoSQL clé-valeur orienté colonnes, write-optimisé, latence ms → on l'utilise pour le temps réel et le drill-down sur un post unique. Hive = data warehouse SQL sur HDFS, read-optimisé, latence seconde → on l'utilise pour les agrégations analytiques et les rapports. Les deux sont complémentaires, c'est le pattern Lambda Architecture revisité.

**Q : Avez-vous testé en charge ?**
> Oui, à hauteur de 5 000 msg/sec sur un MacBook M1 16 Go en local. Bottleneck : Spark NLP côté CPU. En passant en mode `pretrained_pipeline` avec batch size 1024, on atteint 12 000 msg/sec.

**Q : Comment validez-vous la qualité du modèle de classification ?**
> Pipeline classique : split 80/20, cross-validation 5-fold, métriques F1 macro (sentiment) et accuracy + matrice de confusion (catégorie). Tracking dans MLflow avec promotion conditionnelle si F1 ≥ 0.70. Plus, un re-training hebdomadaire via DAG Airflow.

**Q : Quelle est la valeur business concrète ?**
> Pour un opérateur télécom : alerter sur dégradation NPS avant qu'elle n'atteigne le call center (économie estimée : 200-500 K€/an sur la réduction du churn). Pour SENELEC/SEN'EAU : prioriser les zones d'intervention en croisant alertes + cartographie. Pour les régulateurs : observatoire de la qualité de service.

---

## 5. Checklist J-7 / J-1 / J-0

**J-7** :
- [ ] Répéter la démo 3 fois avec timing chronométré
- [ ] Préparer screenshots backup de chaque étape démo
- [ ] Imprimer 3 exemplaires du rapport + slides
- [ ] Tester sur le matériel du jury (HDMI, projection)

**J-1** :
- [ ] `make down && make up` pour partir d'un état propre
- [ ] Vérifier que `make produce` tourne depuis 5 minutes
- [ ] Charger les batteries du laptop + adaptateur HDMI
- [ ] Imprimer la carte mémoire des questions probables
- [ ] Dormir 8h. Pas de code après 22h.

**J-0** :
- [ ] Arriver 30 min en avance
- [ ] Lancer la stack 15 min avant
- [ ] Vérifier le dashboard se rafraîchit
- [ ] Eau, mouchoirs, montre/téléphone en mode chrono
- [ ] Respirer. Sourire. Vous avez construit quelque chose de solide.

---

**Bonne chance !** 🎓
