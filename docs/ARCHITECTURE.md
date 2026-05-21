# 📐 Diagrammes d'Architecture Vox-SN

Tous les diagrammes utilisent la syntaxe **Mermaid**.
Rendu en ligne via https://mermaid.live ou intégrés à GitHub README.

---

## 1. Architecture Globale

```mermaid
flowchart LR
    subgraph SRC["🌍 Sources"]
        TW[Twitter/X]
        FB[Facebook]
        WA[WhatsApp]
        REC[Réclamations]
        SIM[Simulateur Python]
    end

    subgraph ING["📥 Ingestion"]
        NIFI[Apache NiFi<br/>Routage par opérateur]
    end

    subgraph BUS["📨 Message Broker"]
        K1[social_raw]
        K2[social_analyzed]
        K3[social_sentiment_agg]
    end

    subgraph PROC["⚙️ Traitement NLP"]
        SPARK[Spark Streaming<br/>+ Spark NLP<br/>+ Spark MLlib]
        PII[Privacy Layer<br/>SHA-256 + Pandera]
        LEX[Lexique Wolof/FR]
    end

    subgraph STO["💾 Stockage"]
        HBASE[(HBase<br/>Alertes temps réel)]
        HIVE[(Hive<br/>Analytics)]
        HDFS[(HDFS<br/>Modèles ML)]
    end

    subgraph MLOPS["🔄 MLOps"]
        AIR[Airflow<br/>DAGs horaires]
        MLF[MLflow<br/>Tracking]
    end

    subgraph VIZ["📊 Visualisation"]
        DASH[Dashboard HTML]
        REP[Rapport de crise]
    end

    SRC --> NIFI
    NIFI --> K1
    K1 --> SPARK
    SPARK -.utilise.-> PII
    SPARK -.utilise.-> LEX
    SPARK --> K2
    SPARK --> K3
    SPARK --> HBASE
    K2 --> HIVE
    K3 --> HIVE
    AIR -.orchestre.-> SPARK
    SPARK --> MLF
    HIVE --> DASH
    HBASE --> REP
    HDFS --> MLF

    style SPARK fill:#E25A1C,color:#fff
    style NIFI fill:#728E9B,color:#fff
    style K1 fill:#231F20,color:#fff
    style K2 fill:#231F20,color:#fff
    style K3 fill:#231F20,color:#fff
    style HBASE fill:#C72E29,color:#fff
    style HIVE fill:#FDEE21,color:#000
    style AIR fill:#017CEE,color:#fff
    style MLF fill:#0194E2,color:#fff
```

---

## 2. Flux Kafka Détaillé

```mermaid
flowchart TB
    subgraph PROD["👤 Producteurs"]
        P1[kafka_producer_vox.py<br/>simulateur]
        P2[NiFi RouteOnAttribute]
        P3[scripts/inject_crisis.py]
    end

    subgraph TOPICS["📨 Topics Kafka"]
        T1[social_raw<br/>3 partitions]
        T2[social_analyzed<br/>3 partitions]
        T3[social_sentiment_agg<br/>3 partitions]
    end

    subgraph CONS["📥 Consommateurs"]
        C1[Spark Streaming]
        C2[kafka_consumer_check.py]
        C3[Hive Sink]
    end

    P1 -->|JSON brut<br/>avec user_id| T1
    P2 -->|posts routés<br/>key=service| T1
    P3 -->|posts négatifs Wave| T1

    T1 --> C1
    C1 -->|post analysé<br/>sans PII| T2
    C1 -->|agrégats 1h| T3

    T2 --> C2
    T2 --> C3
    T3 --> C3

    style T1 fill:#231F20,color:#fff
    style T2 fill:#231F20,color:#fff
    style T3 fill:#231F20,color:#fff
```

---

## 3. Pipeline Spark NLP Étape par Étape

```mermaid
flowchart TB
    K1[(Kafka: social_raw)] --> R[readStream]
    R --> P[Parse JSON<br/>from_json + schema]
    P --> PII1{Détection PII<br/>regex Sénégal}
    PII1 -->|PII détecté| REJ[❌ Rejet]
    PII1 -->|propre| H[SHA-256<br/>citizen_id_secure]
    H --> DROP["🔒 drop(user_id,<br/>phone_number)"]
    DROP --> N[Nettoyage texte<br/>regex + lower]
    N --> ST[Stopwords FR + Wolof<br/>broadcast]
    ST --> SC[UDF score_sentiment<br/>lexique]
    ST --> CAT[UDF categoriser<br/>4 catégories]
    SC --> LAB[when/otherwise<br/>POSITIF/NEUTRE/NEGATIF/CRISE]
    CAT --> LAB
    LAB --> AGG[window 1h<br/>groupBy service]
    LAB --> W1[(Kafka: social_analyzed)]
    AGG --> W2[(Kafka: social_sentiment_agg)]
    LAB -->|statut=CRISE| HB[(HBase: vox:alertes)]

    style PII1 fill:#fef3c7
    style DROP fill:#fee2e2
    style HB fill:#C72E29,color:#fff
```

---

## 4. DAG Airflow de Monitoring

```mermaid
flowchart LR
    START([▶ start])
    RECALC[recalculate_sentiment<br/>Hive INSERT OVERWRITE]
    DETECT{detect_crises<br/>>3 services en CRISE ?}
    RETRAIN[trigger_retrain<br/>spark-submit train_classifier]
    SKIP[skip_retrain]
    END([⏹ end])

    START --> RECALC
    RECALC --> DETECT
    DETECT -->|oui| RETRAIN
    DETECT -->|non| SKIP
    RETRAIN --> END
    SKIP --> END

    style DETECT fill:#fef3c7
    style RETRAIN fill:#dbeafe
```

---

## 5. Architecture Docker (réseau)

```mermaid
flowchart TB
    subgraph NET["🌐 vox_sn_net (Docker bridge)"]
        subgraph INFRA["Infrastructure"]
            ZK[zookeeper:2181]
            KA[kafka:9092]
            HB[hbase:9090,16010]
        end

        subgraph COMPUTE["Calcul"]
            SM[spark-master:7077,8080]
            SW[spark-worker]
        end

        subgraph CATALOG["Catalogue"]
            HMS[hive-metastore:9083]
            HS[hive-server:10000]
        end

        subgraph ORCH["Orchestration"]
            AF[airflow:8080]
            MLF[mlflow:5000]
        end

        subgraph INGEST["Ingestion"]
            NI[nifi:8080]
            KUI[kafka-ui:8080]
        end
    end

    HOST[💻 Hôte localhost]
    HOST -.8081.-> NI
    HOST -.8082.-> AF
    HOST -.8080.-> SM
    HOST -.8090.-> KUI
    HOST -.5000.-> MLF
    HOST -.16010.-> HB
    HOST -.9092.-> KA

    KA --- ZK
    SM --- SW
    HS --- HMS
    AF -.task spark.-> SM
```

---

## 6. Flux de données complet (timing)

```mermaid
sequenceDiagram
    autonumber
    actor U as 👤 Citoyen
    participant S as Source (Twitter/sim)
    participant N as NiFi
    participant K as Kafka social_raw
    participant SP as Spark NLP
    participant H as HBase
    participant V as Hive
    participant D as Dashboard

    U->>S: Tweet "Wave dafa teye !"
    S->>N: HTTP/REST
    N->>N: Extract service_cible
    N->>K: Publish (key=WAVE)
    Note over K: Latence < 100ms
    K->>SP: readStream micro-batch
    SP->>SP: PII detection
    SP->>SP: SHA-256 hash
    SP->>SP: Lexique score = -0.85
    SP->>SP: Cat = TECHNIQUE → CRISE
    SP->>H: PUT vox:alertes
    SP->>V: INSERT INTO posts_analyses
    Note over SP,V: Latence ingestion → stockage < 5s
    D->>V: SELECT vue_battle_mm
    V-->>D: Données 7 jours
    D->>H: SCAN vox:alertes
    H-->>D: Alertes actives
    Note over D: Refresh dashboard chaque 30s
```

---

## 7. Privacy Layer en profondeur

```mermaid
flowchart LR
    A[Post brut<br/>JSON Kafka] --> B{Validation<br/>Pandera}
    B -->|fail| Q[❌ Quarantaine]
    B -->|ok| C{Regex PII<br/>+221, 7xxxxxxxx,<br/>n° transaction}
    C -->|détecté<br/>dans texte| Q
    C -->|propre| D[Concat user_id + SALT]
    D --> E[SHA-256]
    E --> F[citizen_id_secure]
    F --> G[drop user_id<br/>drop phone_number]
    G --> H[✅ Dataset anonymisé]
    H --> I[(HBase)]
    H --> J[(Hive)]

    style Q fill:#fee2e2
    style E fill:#dbeafe
    style G fill:#fef3c7
    style H fill:#d1fae5
```

---

## Comment exporter en image

### Option 1 : mermaid.live (web)
1. Coller le code Mermaid sur https://mermaid.live
2. Bouton **PNG** ou **SVG** → télécharger

### Option 2 : CLI mermaid-cli
```bash
npm install -g @mermaid-js/mermaid-cli
mmdc -i ARCHITECTURE.md -o docs/diagrams/architecture.png
```

### Option 3 : VS Code
Installer l'extension **Markdown Preview Mermaid Support** → preview rend les diagrammes.
