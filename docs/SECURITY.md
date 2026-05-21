# Sécurité & Privacy by Design — Vox-SN

> Ce document décrit les mesures de protection des données personnelles et de sécurité technique appliquées dans Vox-SN. **Section critique du barème de soutenance (4 pts / 20).**

---

## 1. Cadre réglementaire applicable

### Sénégal
- **Loi n° 2008-12 du 25 janvier 2008** sur la protection des données à caractère personnel
- Autorité de contrôle : **Commission de Protection des Données Personnelles (CDP)** — <https://www.cdp.sn>
- Obligations : déclaration préalable, consentement, droit d'accès / rectification / suppression

### International (pour usage panafricain)
- **RGPD** (UE) : si traitement de citoyens européens
- **Convention de Malabo** (Union Africaine, 2014) : protection cybersécurité + données

### Principes appliqués dans Vox-SN
1. **Minimisation** : ne collecter que ce qui est strictement nécessaire à l'analyse de sentiment
2. **Pseudonymisation** : hash SHA-256 + drop des identifiants en clair
3. **Finalité** : analyse statistique uniquement, pas de re-targeting individuel
4. **Limitation de la conservation** : TTL configuré sur HBase (default 90 jours)
5. **Droit à l'oubli** : procédure de suppression par hash documentée

---

## 2. Détection et traitement des PII (Personally Identifiable Information)

### 2.1 Patterns détectés (`spark/schema.py`)

```python
PII_PATTERNS = {
    "numero_senegal_international": r"\+?221[0-9]{9}",   # +221771234567
    "numero_senegal_local":         r"\b7[0-9]{8}\b",    # 771234567
    "numero_transaction":           r"\b[0-9]{10,16}\b", # codes Wave/OM
    "email":                        r"[\w.+-]+@[\w-]+\.[\w.-]+",
    "carte_bancaire":               r"\b(?:\d[ -]*?){13,19}\b",
}
```

### 2.2 Traitement appliqué

| Détection | Action | Justification |
|---|---|---|
| Numéro de téléphone | Remplacé par `[PHONE_REDACTED]` dans le texte + supprimé de la colonne | Très sensible au Sénégal (mobile money) |
| Numéro de transaction | Remplacé par `[TX_REDACTED]` | Permet retraçage frauduleux |
| Email | Hash SHA-256 si exploitable, sinon redact | Identifiant nominatif |
| Carte bancaire | Redact systématique + alerte | PCI-DSS |

### 2.3 Pipeline d'anonymisation (`spark/streaming_sentiment.py`)

```
Message brut Kafka
   ↓
Validation Pandera (rejet si invalide)
   ↓
Détection PII regex
   ↓
Hash SHA-256(user_id + SALT) → user_hash
Hash SHA-256(phone + SALT)   → phone_hash (si présent)
   ↓
.drop("user_id", "phone_number")   ⚠️ critique
   ↓
Persistance HBase / Hive
```

**Le `.drop()` après hash est non négociable.** Sans cela, les données brutes resteraient en mémoire dans le DataFrame avant écriture.

---

## 3. Gestion du SALT

### Pourquoi un SALT ?
SHA-256(`771234567`) est trivialement réversible par dictionnaire (10⁹ combinaisons sur 9 chiffres = quelques minutes de brute-force). Un SALT secret rend l'attaque impossible.

### Configuration
```bash
# .env (NE PAS COMMITER)
SALT=UADB_VOX_2025_a8f3k9p2m7q1r4

# Production recommandée :
# - SALT stocké dans HashiCorp Vault ou AWS Secrets Manager
# - Rotation tous les 6 mois
# - Hash HMAC-SHA256 plutôt que SHA-256 brut
```

### Bonnes pratiques
- `.env` est dans `.gitignore` — vérifier avant push
- `SALT` de 16+ caractères, alphanumérique mixte
- Si compromis, re-hash de toute la base + invalidation des hash existants

---

## 4. Sécurité réseau & accès

### 4.1 Isolation Docker
Tous les services tournent sur le réseau Docker `vox_sn_net`. Aucun port n'est exposé en dehors de l'hôte sauf :
- Interfaces UI (NiFi 8081, Spark 8080, Airflow 8082, MLflow 5000, Kafka UI 8090, HBase 16010)
- Kafka 9092 (pour clients hors Docker)

### 4.2 Authentification
- **NiFi** : `admin` / mot de passe 16 caractères dans `.env`
- **Airflow** : utilisateur admin créé via `airflow users create`
- **HBase / Hive / Kafka** : authentification désactivée en dev — **À ACTIVER EN PROD** (SASL/PLAIN, Kerberos)

### 4.3 Recommandations production
- TLS partout (Kafka, HBase, Hive, dashboards)
- Reverse proxy nginx + WAF devant les UI
- Audit logs sur toutes les actions admin (auth.log)
- Mise à jour mensuelle des images Docker (CVE)

---

## 5. Droits des personnes concernées

### Procédure « droit à l'oubli »
Un utilisateur demande la suppression de ses données :

1. Récupérer son `user_hash` :
   ```python
   user_hash = sha256(f"{user_id}{SALT}".encode()).hexdigest()
   ```
2. Supprimer dans HBase :
   ```bash
   hbase shell
   > deleteall 'vox:posts', '<user_hash>'
   ```
3. Marquer en Hive (soft delete) :
   ```sql
   INSERT INTO vox_sn.gdpr_deletions VALUES ('<user_hash>', NOW());
   ```
4. Le DAG `vox_sn_ingestion_dag` exclut ensuite les `user_hash` listés

### Procédure « droit d'accès »
- Requête HBase par `user_hash` → export JSON des posts associés
- Délai légal Sénégal : 30 jours

---

## 6. Audit & traçabilité

### 6.1 Logs applicatifs
| Composant | Localisation | Rotation |
|---|---|---|
| Kafka | `docker logs vox_kafka` | 7 jours |
| Spark | `airflow/logs/streaming/` | 30 jours |
| Airflow | `airflow/logs/dag_id/` | 90 jours |
| MLflow | `mlflow/artifacts/` | indéfini |

### 6.2 Audit Privacy
Une table Hive `vox_sn.audit_pii` enregistre :
- Date du traitement
- Nombre de PII détectées / redactées
- Patterns matchés
- Volume traité

Requête mensuelle :
```sql
SELECT date_jour, SUM(nb_pii_detectees), SUM(nb_messages_traites)
FROM vox_sn.audit_pii
WHERE date_jour >= date_sub(current_date, 30)
GROUP BY date_jour
ORDER BY date_jour;
```

---

## 7. Menaces & contre-mesures

| Menace | Impact | Contre-mesure Vox-SN | Statut |
|---|---|---|---|
| Fuite base utilisateurs | Critique | Hash SHA-256 + drop colonnes | ✅ Implémenté |
| Re-identification par croisement | Élevé | SALT secret + minimisation | ✅ Implémenté |
| Injection SQL via texte | Moyen | Spark DataFrames (pas de string interpolation) | ✅ Implémenté |
| DoS sur producer Kafka | Moyen | Rate limiting à 1000 msg/s/IP | ⚠️ À configurer prod |
| Compromission SALT | Critique | Vault + rotation 6 mois | ⚠️ À implémenter prod |
| Accès NiFi non autorisé | Élevé | TLS + auth + audit | ⚠️ Auth basique en dev |
| Logs avec PII | Moyen | Filtres Spark + audit | ✅ Implémenté |

---

## 8. Checklist de conformité avant mise en production

- [ ] Déclaration CDP Sénégal effectuée
- [ ] DPO (Délégué à la Protection des Données) désigné
- [ ] Registre des traitements à jour
- [ ] Procédures « droit d'accès » et « droit à l'oubli » testées
- [ ] SALT stocké dans Vault, non commit
- [ ] TLS activé sur tous les services
- [ ] Authentification renforcée (Kerberos / LDAP / SSO)
- [ ] Audit logs centralisés (ELK / Datadog)
- [ ] Pentest réalisé (OWASP Top 10)
- [ ] Backup chiffré + plan de reprise (RTO < 4h, RPO < 1h)
- [ ] Formation équipe sur la loi 2008-12

---

## 9. Références

- Loi sénégalaise 2008-12 : <https://www.cdp.sn/sites/default/files/2018-10/loi-2008-12.pdf>
- RGPD : <https://gdpr-info.eu/>
- OWASP Top 10 : <https://owasp.org/www-project-top-ten/>
- NIST Privacy Framework : <https://www.nist.gov/privacy-framework>
- Convention de Malabo : <https://au.int/en/treaties/african-union-convention-cyber-security-and-personal-data-protection>

---

**Document à présenter au jury à la slide 6 de la soutenance.**
