# Data Model - Spotify Pipeline

## Entity Relationship Diagram (ERD)

```mermaid
erDiagram
    GENRES {
        int id PK
        string name
        timestamp created_at
    }

    ARTISTS {
        uuid id PK
        string name
        string country
        string label
        text_array genres
        int monthly_listeners
        timestamp created_at
        timestamp updated_at
    }

    ALBUMS {
        uuid id PK
        uuid artist_id FK
        string title
        int release_year
        int total_tracks
        timestamp created_at
    }

    TRACKS {
        uuid id PK
        uuid album_id FK
        uuid artist_id FK
        string title
        int duration_ms
        string genre
        int bpm
        boolean explicit
        string audio_file_path
        timestamp created_at
        timestamp updated_at
    }

    PEERS {
        uuid id PK
        string peer_name
        string ip_address
        string device_type
        string geo_country
        string geo_city
        string status
        text_array cached_tracks
        timestamp last_seen
        timestamp created_at
    }

    LISTENING_EVENTS {
        uuid id PK
        uuid user_id
        uuid track_id FK
        uuid source_peer_id FK
        timestamp timestamp
        int duration_ms
        string device_type
        string geo_country
        boolean completed
        string event_source
        timestamp created_at
    }

    DAILY_STREAMS {
        uuid track_id PK, FK
        date date PK
        bigint total_streams
        bigint unique_listeners
        bigint total_duration_ms
        text_array countries
        timestamp updated_at
    }

    ARTIST_STATS {
        uuid artist_id PK, FK
        date date PK
        bigint total_streams
        bigint unique_listeners
        uuid top_track_id
        timestamp updated_at
    }

    REALTIME_TOP_TRACKS {
        timestamp window_start PK
        timestamp window_end
        uuid track_id PK, FK
        bigint stream_count
        bigint unique_listeners
        timestamp updated_at
    }

    DEAD_LETTER_EVENTS {
        uuid id PK
        string original_topic
        jsonb payload
        string error_type
        text error_message
        int retry_count
        string status
        timestamp created_at
        timestamp last_retry_at
        timestamp resolved_at
    }

    FRAUD_DETECTIONS {
        uuid id PK
        uuid user_id
        uuid peer_id
        string fraud_type
        float suspicion_score
        jsonb evidence
        timestamp window_start
        timestamp window_end
        timestamp detected_at
    }

    RECOMMENDATIONS {
        uuid user_id PK
        uuid track_id PK, FK
        float score
        timestamp generated_at
    }

    FEDERATED_CATALOG {
        uuid track_id PK
        string source_group PK
        string artist_name
        string track_title
        int duration_ms
        string genre
        string audio_peer_endpoint
        timestamp ingested_at
    }

    ARTISTS ||--o{ ALBUMS : "creates"
    ARTISTS ||--o{ TRACKS : "performs"
    ALBUMS ||--o{ TRACKS : "contains"
    TRACKS ||--o{ LISTENING_EVENTS : "is played in"
    PEERS ||--o{ LISTENING_EVENTS : "serves"
    TRACKS ||--o{ DAILY_STREAMS : "aggregated into"
    TRACKS ||--o{ REALTIME_TOP_TRACKS : "tracked in"
    ARTISTS ||--o{ ARTIST_STATS : "summarized in"
```

## Questions & Answers

### 1. Pourquoi `listening_events` est indexé sur `(timestamp)` ET `date_trunc('hour', timestamp)` ?

*   **`idx_listening_events_timestamp`** : Cet index est crucial pour les requêtes basées sur des plages de temps (ex: `WHERE timestamp > NOW() - INTERVAL '1 day'`). Il permet au moteur SQL de localiser rapidement les lignes dans une fenêtre temporelle continue.
*   **`idx_listening_events_ts_partition`** : Il s'agit d'un **index fonctionnel**. Dans un pipeline de données, on agrège très souvent par heure (`GROUP BY date_trunc('hour', timestamp)`). Sans cet index, PostgreSQL devrait calculer la fonction pour chaque ligne avant de pouvoir regrouper. Avec cet index, il utilise directement les valeurs pré-calculées, ce qui accélère drastiquement les agrégations horaires et permet d'optimiser le "partition pruning" logique si des vues ou des requêtes filtrent par tranches horaires.

### 2. Quelle est la différence entre `daily_streams` (batch) et `realtime_top_tracks` (Spark) ?

*   **`daily_streams` (Batch)** : C'est la table de vérité historique. Elle est alimentée par un processus batch (probablement Airflow) qui tourne une fois par jour. Elle consolide les données sur 24h avec une précision maximale, incluant éventuellement des corrections de données tardives. Elle sert au reporting officiel et au calcul des royalties.
*   **`realtime_top_tracks` (Spark Streaming)** : C'est une table de tendance à chaud. Elle est alimentée par Spark Structured Streaming avec des fenêtres glissantes ou fixes de 5 minutes. Elle permet de voir ce qui "buzze" à l'instant T sur la plateforme, mais elle n'a pas vocation à être la source comptable finale (elle peut être sujette à de petits écarts dus au temps réel).

### 3. Pourquoi `dead_letter_events.payload` est JSONB plutôt que TEXT ?

*   **Performance et Structure** : `JSONB` stocke le JSON dans un format binaire décomposé. Contrairement au `TEXT` qui nécessite un parsing complet à chaque lecture, le `JSONB` permet un accès rapide aux champs.
*   **Capacités de Requêtage** : On peut indexer le contenu du `JSONB` (index GIN). Cela permet de faire des recherches complexes dans la DLQ, comme par exemple : "Trouve tous les événements en échec qui concernent le `track_id` X ou qui viennent du `peer_id` Y", directement en SQL avec les opérateurs `@>` ou `->>`.
*   **Validation** : Le type `JSONB` garantit que les données insérées sont du JSON valide, évitant ainsi d'avoir des payloads corrompus dans la file d'erreur.
