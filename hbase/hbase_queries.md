# Requêtes HBase — Vox-SN

Référence rapide des commandes HBase shell pour la démo et le développement.

---

## Connexion

```bash
docker exec -it vox_hbase hbase shell
```

---

## 1. Listing & schémas

### Lister toutes les tables
```
list
```
Attendu :
```
TABLE
vox:posts
vox:alertes
vox:sentiment_agg
3 row(s)
```

### Décrire une table
```
describe 'vox:posts'
```

---

## 2. Lecture (scan & get)

### Compter les lignes
```
count 'vox:posts', INTERVAL => 10000
```

### Scan limité avec colonnes filtrées
```
scan 'vox:posts', {LIMIT => 5, COLUMNS => ['nlp:texte_clean', 'nlp:sentiment_score', 'meta:service_cible']}
```

### Get un post précis (preuve anonymisation — ⭐ capture 13)
```
get 'vox:posts', 'POST_000001'
```
Attendu : colonnes `meta:*`, `nlp:*`, `privacy:citizen_id_secure` — **sans** `user_id` ni `phone_number`.

### Get une colonne spécifique
```
get 'vox:posts', 'POST_000001', 'nlp:sentiment_score'
```

---

## 3. Filtres avancés

### Posts d'un service donné (Wave)
```
scan 'vox:posts', {
  FILTER => "SingleColumnValueFilter('analysis', 'service', =, 'binary:WAVE')",
  LIMIT => 10
}
```

### Posts négatifs (score < -0.5)
```
scan 'vox:posts', {
  FILTER => "SingleColumnValueFilter('analysis', 'sentiment_score', <, 'binary:-0.5')",
  LIMIT => 20
}
```

### Posts en Wolof uniquement
```
scan 'vox:posts', {
  FILTER => "SingleColumnValueFilter('content', 'lang', =, 'binary:wo')",
  LIMIT => 10
}
```

### Posts sur une fenêtre temporelle (timestamps)
```
scan 'vox:posts', {
  TIMERANGE => [1715760000000, 1715846400000],
  LIMIT => 50
}
```

---

## 4. Alertes (vox:alertes)

### Lister les alertes actives
```
scan 'vox:alertes', {
  FILTER => "SingleColumnValueFilter('meta', 'status', =, 'binary:OPEN')"
}
```

### Top 5 alertes les plus récentes
```
scan 'vox:alertes', {LIMIT => 5, REVERSED => true}
```

### Compter les alertes par service
```bash
# Depuis le shell HBase, plus complexe → utiliser une requête Hive à la place :
# SELECT service, COUNT(*) FROM vox_sn.alertes GROUP BY service;
```

---

## 5. Agrégations (vox:sentiment_agg)

### Score moyen agrégé par service (clés rowkey = service#date#heure)
```
scan 'vox:sentiment_agg', {
  STARTROW => 'WAVE#20260515',
  STOPROW  => 'WAVE#20260516',
  COLUMNS  => ['agg:avg_score', 'agg:nb_messages']
}
```

### Tendance horaire SENELEC sur 24h
```
scan 'vox:sentiment_agg', {
  STARTROW => 'SENELEC#20260515#00',
  STOPROW  => 'SENELEC#20260515#23',
  COLUMNS  => ['agg:avg_score']
}
```

---

## 6. Insertions manuelles (tests)

### Ajouter un post de test
```
put 'vox:posts', 'POST_TEST_001', 'content:text', 'Wave dafa baax torop !'
put 'vox:posts', 'POST_TEST_001', 'content:lang', 'wo'
put 'vox:posts', 'POST_TEST_001', 'analysis:service', 'WAVE'
put 'vox:posts', 'POST_TEST_001', 'analysis:sentiment_score', '0.85'
put 'vox:posts', 'POST_TEST_001', 'analysis:category', 'POSITIF'
put 'vox:posts', 'POST_TEST_001', 'meta:user_hash', 'a8f3k9p2m7q1r4...'
```

### Ajouter une alerte
```
put 'vox:alertes', 'ALERT_001', 'meta:service', 'SENELEC'
put 'vox:alertes', 'ALERT_001', 'meta:severity', 'HIGH'
put 'vox:alertes', 'ALERT_001', 'meta:status', 'OPEN'
put 'vox:alertes', 'ALERT_001', 'data:avg_score', '-0.72'
put 'vox:alertes', 'ALERT_001', 'data:nb_messages', '342'
put 'vox:alertes', 'ALERT_001', 'data:trigger_time', '2026-05-15T08:30:00Z'
```

---

## 7. Suppression (RGPD / droit à l'oubli)

### Supprimer un post
```
delete 'vox:posts', 'POST_000001', 'content:text'
deleteall 'vox:posts', 'POST_000001'
```

### Supprimer toutes les données d'un utilisateur (par user_hash)
```
scan 'vox:posts', {
  FILTER => "SingleColumnValueFilter('meta', 'user_hash', =, 'binary:HASH_A_SUPPRIMER')",
  COLUMNS => ['meta:user_hash']
}
# Puis pour chaque rowkey trouvée :
deleteall 'vox:posts', '<rowkey>'
```

---

## 8. Maintenance

### Compacter une table (manuel)
```
major_compact 'vox:posts'
```

### Stats d'une table
```
status 'vox:posts'
```

### Désactiver / activer (rare en prod)
```
disable 'vox:posts'
enable 'vox:posts'
```

---

## 9. Démo guidée (5 commandes clés pour la soutenance)

```
# 1. Montrer la structure
list

# 2. Compter le volume ingéré
count 'vox:posts', INTERVAL => 10000

# 3. Récupérer un post anonymisé (NO user_id NO phone_number !)
get 'vox:posts', 'POST_000001'

# 4. Filtrer les messages négatifs en Wolof
scan 'vox:posts', {
  FILTER => "SingleColumnValueFilter('meta', 'langue', =, 'binary:WO') AND SingleColumnValueFilter('nlp', 'sentiment_score', <, 'binary:-0.5')",
  LIMIT => 10
}

# 5. Lister les alertes ouvertes
scan 'vox:alertes', {FILTER => "SingleColumnValueFilter('meta', 'status', =, 'binary:OPEN')"}
```

---

## 10. Accès programmatique (Python via happybase)

```python
import happybase

conn = happybase.Connection('localhost', port=9090)
table = conn.table('vox:posts')

# Get
row = table.row(b'POST_000001')
print(row[b'analysis:sentiment_score'])

# Scan avec filtre
for key, data in table.scan(
    filter=b"SingleColumnValueFilter('analysis', 'service', =, 'binary:WAVE')",
    limit=10
):
    print(key, data[b'content:text'])

conn.close()
```

---

**Astuce démo** : ouvrir HBase shell dans une fenêtre dédiée AVANT la présentation. Si la connexion plante, faire le même résultat depuis Hive (plus stable, syntaxe SQL).
