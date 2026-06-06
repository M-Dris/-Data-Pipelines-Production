"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    - Redis      → clé `genre_listeners:live` (top genres par sliding window)
    - Kafka      → topic `late_listening_events` (events tardifs > 10 min)

Watermarking :
    Chaque fenêtre applique un watermark de 10 minutes. Les events dont
    l'event_time est antérieur de plus de 10 min à l'heure courante sont
    détectés et routés vers le topic late_listening_events plutôt qu'ignorés.

Checkpoints MinIO (exactly-once) :
    Les checkpoints sont stockés sur MinIO (s3a://spotify-checkpoints/*)
    pour garantir l'exactly-once après redémarrage Spark.
    Vérification : SELECT COUNT(*) - COUNT(DISTINCT id) AS doublons FROM listening_events; → 0

Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1,\\
                   org.apache.hadoop:hadoop-aws:3.3.4,\\
                   com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
        spark_jobs/streaming_trends_job.py

Simulateur late events :
    python -m src.p2p_simulator.simulator --mode late_events
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
KAFKA_TOPIC       = "listening_events"
LATE_EVENTS_TOPIC = "late_listening_events"

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

CHECKPOINT_PATH   = "s3a://spotify-checkpoints/streaming_trends"
CHECKPOINT_GENRES = "s3a://spotify-checkpoints/streaming_genres"
CHECKPOINT_LATE   = "s3a://spotify-checkpoints/late_events"

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "spotify")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "spotify")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "spotify")
POSTGRES_URL      = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPS    = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),  False),
    StructField("user_id",      StringType(),  False),
    StructField("track_id",     StringType(),  False),
    StructField("source_peer",  StringType(),  True),
    StructField("timestamp",    StringType(),  False),
    StructField("duration_ms",  IntegerType(), True),
    StructField("device_type",  StringType(),  True),
    StructField("geo_country",  StringType(),  True),
    StructField("completed",    BooleanType(), True),
    StructField("event_source", StringType(),  True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka listening_events en streaming.
    Parse le JSON et caste le timestamp en event_time.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed")  # Issue #16 : ne lit que les messages committés (EOS)
        .load()
    )
    return (
        raw
        .select(
            F.from_json(
                F.col("value").cast("string"),
                LISTENING_EVENT_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn("event_time", F.col("timestamp").cast(TimestampType()))
        .drop("timestamp")
        .withWatermark("event_time", "10 minutes")
    )


# ─────────────────────────────────────────────────────────────
# ROUTAGE LATE EVENTS
# ─────────────────────────────────────────────────────────────

def route_late_events(events_df):
    """
    Détecte les events tardifs (event_time < now - 10 min) et les publie
    dans le topic Kafka late_listening_events pour traitement différé.

    Le watermark de 10 min définit le seuil : un event est considéré tardif
    si son event_time est antérieur de plus de 10 minutes à l'heure courante.
    Utilise le sink Kafka natif Spark (spark-sql-kafka), sans dépendance externe.
    """
    late_df = events_df.filter(
        F.expr("event_time < current_timestamp() - INTERVAL 10 MINUTES")
    )

    late_kafka = late_df.withColumn(
        "value",
        F.to_json(F.struct([F.col(c) for c in late_df.columns]))
    ).select("value")

    return (
        late_kafka.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", LATE_EVENTS_TOPIC)
        .option("checkpointLocation", CHECKPOINT_LATE)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top 10 tracks par tumbling window de 5 minutes.
    Écrit dans PostgreSQL via psycopg2 avec UPSERT.
    """
    windowed = (
        events_df
        .filter(F.col("completed") == True)
        .groupBy(
            F.window(F.col("event_time"), "5 minutes"),
            F.col("track_id")
        )
        .agg(
            F.count("*").alias("stream_count"),
            F.approx_count_distinct("user_id").alias("unique_listeners")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("track_id"),
            F.col("stream_count"),
            F.col("unique_listeners")
        )
    )

    def write_to_postgres(batch_df, batch_id):
        if batch_df.isEmpty():
            print(f"[Batch {batch_id}] Aucun événement, skip.")
            return

        from pyspark.sql.window import Window
        import psycopg2 as pg

        window_spec = Window.partitionBy("window_start").orderBy(
            F.col("stream_count").desc()
        )
        top10 = (
            batch_df
            .withColumn("rank", F.row_number().over(window_spec))
            .filter(F.col("rank") <= 10)
            .drop("rank")
            .withColumn("track_id", F.col("track_id").cast("string"))
        )

        rows = top10.collect()

        conn = pg.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB, options="-c client_encoding=UTF8"
        )
        cursor = conn.cursor()
        for row in rows:
            cursor.execute("""
                INSERT INTO realtime_top_tracks
                    (window_start, window_end, track_id,
                     stream_count, unique_listeners, updated_at)
                VALUES (%s, %s, %s::uuid, %s, %s, NOW())
                ON CONFLICT (window_start, track_id) DO UPDATE SET
                    stream_count     = EXCLUDED.stream_count,
                    unique_listeners = EXCLUDED.unique_listeners,
                    updated_at       = NOW()
            """, (
                row["window_start"], row["window_end"], row["track_id"],
                row["stream_count"], row["unique_listeners"]
            ))
        conn.commit()
        cursor.close()
        conn.close()

        print(f"[Batch {batch_id}] {len(rows)} lignes → realtime_top_tracks")

    return (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
    )


def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Listeners uniques par genre en sliding window (15 min / slide 5 min).
    Écrit dans Redis clé genre_listeners:live.
    """
    enriched = (
        events_df
        .filter(F.col("completed") == True)
        .join(
            catalog_df.select("id", "genre"),
            events_df.track_id == catalog_df.id,
            how="left"
        )
        .filter(F.col("genre").isNotNull())
    )

    windowed = (
        enriched
        .groupBy(
            F.window(F.col("event_time"), "15 minutes", "5 minutes"),
            F.col("genre")
        )
        .agg(
            F.approx_count_distinct("user_id").alias("unique_listeners")
        )
    )

    def write_to_redis(batch_df, batch_id):
        import redis
        import json

        if batch_df.isEmpty():
            print(f"[Batch genres {batch_id}] Aucun événement, skip.")
            return

        rows = batch_df.collect()

        genre_stats = {}
        for row in rows:
            genre = row["genre"]
            listeners = row["unique_listeners"]
            if genre not in genre_stats or listeners > genre_stats[genre]:
                genre_stats[genre] = listeners

        sorted_genres = dict(
            sorted(genre_stats.items(), key=lambda x: x[1], reverse=True)
        )

        print(f"[Batch genres {batch_id}] {len(sorted_genres)} genres → Redis")

        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=1,
                            decode_responses=True)
            r.setex(
                "genre_listeners:live",
                300,
                json.dumps(sorted_genres)
            )
            print(f"[Batch genres {batch_id}] Redis OK : {sorted_genres}")
        except Exception as e:
            print(f"[Batch genres {batch_id}] Erreur Redis : {e}")

    return (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_GENRES)
        .trigger(processingTime="30 seconds")
        .start()
    )


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job — Issue #15")
    print(f"Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint top tracks : {CHECKPOINT_PATH}")
    print(f"Checkpoint genres     : {CHECKPOINT_GENRES}")

    events_df = read_kafka_stream(spark)

    # Issue #13 : sortie console pour valider la lecture du topic
    query_console = (
        events_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    # Issue #16 : écriture des event_id dans PostgreSQL pour vérifier l'exactly-once
    # INSERT ... ON CONFLICT DO NOTHING → idempotent, aucun doublon possible
    def write_event_ids(batch_df, batch_id):
        (
            batch_df.select("event_id")
            .write.format("jdbc")
            .option("url", POSTGRES_URL)
            .option("dbtable", "streaming_event_ids")
            .option("driver", POSTGRES_PROPS["driver"])
            .option("user", POSTGRES_PROPS["user"])
            .option("password", POSTGRES_PROPS["password"])
            .mode("append")
            .save()
        )
        print(f"[Batch {batch_id}] {batch_df.count()} event_ids écrits dans streaming_event_ids")

    query_ids = (
        events_df.writeStream
        .foreachBatch(write_event_ids)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH + "_ids")
        .start()
    )

    # Issue #14 : agrégations (décommenter)
    # catalog_df = spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)
    # query_top_tracks = compute_top_tracks_tumbling(events_df)
    # query_genres     = compute_genre_listeners_sliding(events_df, catalog_df)
    catalog_df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="tracks",
        properties=POSTGRES_PROPS
    )
    print(f"Catalogue chargé : {catalog_df.count()} tracks")

    query_top_tracks = compute_top_tracks_tumbling(events_df)
    query_genres     = compute_genre_listeners_sliding(events_df, catalog_df)
    query_late       = route_late_events(events_df)

    print("Trois queries streaming démarrées :")
    print(f"  - top_tracks  : {query_top_tracks.id}")
    print(f"  - genres      : {query_genres.id}")
    print(f"  - late_events : {query_late.id}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
