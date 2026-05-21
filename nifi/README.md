# NiFi — Configuration du flux Vox-SN

## 1. Connexion à l'UI

```bash
# Démarrer NiFi
docker compose up -d nifi

# Récupérer les identifiants générés
docker logs vox_nifi 2>&1 | grep -E "Generated (User|Password)"
```

Puis ouvrir https://localhost:8081/nifi (HTTPS par défaut sur NiFi 1.23).

Identifiants par défaut (définis dans `docker-compose.yml`) :
- **User** : `admin`
- **Password** : `voxsnadminpwd2025` (min 12 caractères requis par NiFi)

## 2. Import du template

1. Cliquer sur l'icône **Operate Palette** (à droite de l'interface)
2. **Upload Template** → sélectionner `nifi/templates/vox_sn_routing.xml`
3. Glisser l'icône **Template** dans le canvas (icône représentant des post-it)
4. Choisir **vox_sn_routing** dans la liste

## 3. Activation du flux

1. Sélectionner tous les processors (Ctrl+A)
2. Clic droit → **Start**
3. Les flèches doivent passer au vert (running)

## 4. Architecture du flux

```
[TailFile] → [EvaluateJsonPath] → [RouteOnAttribute] → [PublishKafka]
   ↓                  ↓                  ↓                  ↓
  Lit          Extrait        6 routes par      Topic
data/posts   service_cible    opérateur       social_raw
```

## 5. Sources alternatives

Pour brancher d'autres sources (réseaux sociaux réels), remplacer le `TailFile` par :
- **ConsumeTwitter** : flux Twitter API v2
- **GetHTTP** : webhook réclamations
- **ConsumeKafka_2_6** : autre topic Kafka entrant
- **ListenHTTP** : endpoint REST pour WhatsApp Business API

## 6. Variables d'environnement

Le flux utilise ces variables (à définir dans **Controller Settings → Parameter Contexts**) :

| Variable             | Valeur           |
|----------------------|------------------|
| `kafka.brokers`      | `kafka:9092`     |
| `posts.input.dir`    | `/opt/nifi/data/posts` |
| `kafka.topic`        | `social_raw`     |
