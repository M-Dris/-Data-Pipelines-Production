"""
SPOTIFY — Simulateur P2P
========================
Ce simulateur génère des événements réalistes d'un réseau peer-to-peer
de streaming musical. Il publie dans Redis pub/sub (Phase 1) et dans
Kafka (Phase 2, après décommentage).

Usage :
    python -m src.p2p_simulator.simulator --peers 10 --rate 5
    python -m src.p2p_simulator.simulator --mode fraud --peers 5
    python -m src.p2p_simulator.simulator --mode late_events

Phase 1 :  _generate_listening_event() et _publish_to_redis() — implémentés
Phase 2 :  _publish_to_kafka() activé (dual publish Redis + Kafka)
"""
from dotenv import load_dotenv
load_dotenv(encoding="utf-8-sig")


import argparse
import json
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import redis
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("p2p_simulator")


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

REDIS_URL = "redis://localhost:6379/1"
# External listeners (host → Docker): localhost:29092,localhost:29094,localhost:29096
KAFKA_BOOTSTRAP = "localhost:29092,localhost:29094,localhost:29096"

TOPICS = {
    "listening":   "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]  # pondéré : 60% P2P


# ─────────────────────────────────────────────────────────────
# DONNÉES SIMULÉES
# ─────────────────────────────────────────────────────────────

# Ces UUIDs seront remplacés par les vrais IDs depuis PostgreSQL
# Une fois votre base peuplée, charger dynamiquement avec _load_catalog()
SAMPLE_TRACKS = [
    {"id": str(uuid.uuid4()), "title": f"Track {i}", "duration_ms": random.randint(120000, 300000)}
    for i in range(50)
]

SAMPLE_USERS = [str(uuid.uuid4()) for _ in range(200)]
SAMPLE_PEERS = [str(uuid.uuid4()) for _ in range(20)]


# ─────────────────────────────────────────────────────────────
# SIMULATEUR PRINCIPAL
# ─────────────────────────────────────────────────────────────

class P2PSimulator:
    """
    Simulateur du réseau P2P SPOTIFY.

    Génère deux types d'événements :
    - listening_events   : un utilisateur écoute un morceau via un peer
    - p2p_network_events : connexion/déconnexion/transfert entre peers
    """

    def __init__(
        self,
        n_peers: int = 10,
        events_per_second: float = 5.0,
        mode: str = "normal",
    ):
        self.n_peers = n_peers
        self.events_per_second = events_per_second
        self.mode = mode
        self.running = True
        self.event_count = 0
        self.redis_available = False

        # Charger le catalogue PostgreSQL
        self._load_catalog()


        # Connexion Redis
        self.redis = redis.from_url(REDIS_URL, decode_responses=True, socket_keepalive=True)
        try:
            self.redis.ping()
            self.redis_available = True
        except Exception as e:
            logger.error(f"Connexion Redis indisponible au démarrage ({REDIS_URL}) : {e}")

        # Kafka producer (Phase 2)
        self.kafka_producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 60000,
            "reconnect.backoff.max.ms": 5000,
        })
        logger.info(f"Kafka producer initialisé ({KAFKA_BOOTSTRAP})")

        # Peers actifs simulés
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(f"Simulateur démarré | mode={mode} | peers={n_peers} | rate={events_per_second} evt/s")

    def run(self):
        """Boucle principale : génère et publie des événements en continu."""
        interval = 1.0 / self.events_per_second

        while self.running:
            try:
                # Alterner listening et réseau P2P (80% / 20%)
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(f"Événements publiés : {self.event_count}")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur lors de la génération d'événement : {e}")
                time.sleep(1)

    def _load_catalog(self):
        """
        Connecte à PostgreSQL et remplace SAMPLE_TRACKS par les vraies données.
        Lève une exception si la connexion échoue ou si la table est vide.
        """
        global SAMPLE_TRACKS
        import psycopg2
        import os

        host = os.getenv("POSTGRES_HOST", "127.0.0.1").strip()
        port = os.getenv("POSTGRES_PORT", "5432").strip()
        user = os.getenv("POSTGRES_USER", "spotify").strip()
        password = os.getenv("POSTGRES_PASSWORD", "spotify").strip()
        dbname = os.getenv("POSTGRES_DB", "spotify").strip()

        print(f"[DEBUG] Tentative de connexion PostgreSQL : {user}@{host}:{port}/{dbname}")

        try:
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password, dbname=dbname,
                options="-c client_encoding=UTF8"
            )
            print("[DEBUG] Connexion PostgreSQL établie.")
        except Exception as e:
            print(f"[DEBUG] Échec de connexion PostgreSQL : {e}")
            raise RuntimeError(
                f"Impossible de se connecter à PostgreSQL ({user}@{host}:{port}/{dbname}) : {e}"
            ) from e

        cursor = conn.cursor()
        cursor.execute("SELECT id, duration_ms FROM tracks LIMIT 1000;")
        tracks = cursor.fetchall()
        cursor.close()
        conn.close()

        print(f"[DEBUG] Requête exécutée, {len(tracks)} morceaux récupérés.")

        if not tracks:
            raise RuntimeError(
                "La table tracks est vide — lance d'abord catalog_ingestion_pipeline "
                "avant de démarrer le simulateur."
            )

        SAMPLE_TRACKS = [{"id": str(t[0]), "duration_ms": t[1]} for t in tracks]
        print(f"[DEBUG] Catalogue chargé : {len(SAMPLE_TRACKS)} morceaux.")

    # ── Génération d'événements ──────────────────────────────

    def _generate_listening_event(self) -> dict:
        """
        Génère un événement d'écoute.

        TODO : compléter ce squelette pour générer un événement réaliste.
        Champs attendus :
            - event_id     : UUID unique
            - user_id      : UUID utilisateur (depuis SAMPLE_USERS)
            - track_id     : UUID du morceau (depuis SAMPLE_TRACKS)
            - source_peer  : UUID du peer qui sert le morceau
            - timestamp    : ISO 8601 (datetime.utcnow())
            - duration_ms  : durée écoutée (entre 30 000 et track.duration_ms)
            - device_type  : depuis DEVICE_TYPES
            - geo_country  : depuis GEO_COUNTRIES
            - completed    : bool (True si duration_ms > 30s)
            - event_source : depuis EVENT_SOURCES

        En mode "fraud" (Phase 2) :
            - 30% des events : duration_ms < 5000 (écoute trop courte = bot)
            - 10% : même user_id sur 20 tracks en <10 secondes

        En mode "late_events" (Phase 2) :
            - timestamp décalé de -5 à -30 minutes dans le passé
        """
        track = random.choice(SAMPLE_TRACKS)
        user_id = random.choice(SAMPLE_USERS)
        source_peer = random.choice(self.active_peers)
        duration_ms = random.randint(30000, track["duration_ms"])

        if self.mode == "fraud" and random.random() < 0.3:
            duration_ms = random.randint(100, 4999)

        timestamp = datetime.utcnow()
        if self.mode == "late_events" and random.random() < 0.4:
            delay_minutes = random.randint(5, 30)
            timestamp = timestamp - timedelta(minutes=delay_minutes)

        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "track_id": track["id"],
            "source_peer": source_peer,
            "timestamp": timestamp.isoformat() + "Z",
            "duration_ms": duration_ms,
            "device_type": random.choice(DEVICE_TYPES),
            "geo_country": random.choice(GEO_COUNTRIES),
            "completed": duration_ms > 30000,
            "event_source": random.choice(EVENT_SOURCES),
        }
        return event

    def _generate_p2p_network_event(self) -> dict:
        """
        Génère un événement réseau P2P.

        TODO : compléter pour générer des événements de type :
            - peer_connect    : un peer rejoint le réseau
            - peer_disconnect : un peer quitte le réseau
            - chunk_transfer  : transfert d'un chunk audio entre peers
            - cache_hit       : le morceau était en cache local
            - cache_miss      : téléchargement depuis un autre peer nécessaire
        """
        event_type = random.choice([
            "peer_connect", "peer_disconnect",
            "chunk_transfer", "cache_hit", "cache_miss"
        ])

        peer_id = random.choice(self.active_peers)
        timestamp = datetime.utcnow().isoformat() + "Z"
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id": peer_id,
            "timestamp": timestamp,
        }

        if event_type == "peer_connect":
            event["status"] = "connected"
            event["connection_type"] = random.choice(["bootstrap", "relay", "direct"])
        elif event_type == "peer_disconnect":
            event["status"] = "disconnected"
            event["reason"] = random.choice(["idle_timeout", "manual_leave", "network_loss"])
        elif event_type == "chunk_transfer":
            target_peer = random.choice([p for p in self.active_peers if p != peer_id] or self.active_peers)
            track = random.choice(SAMPLE_TRACKS)
            event["target_peer"] = target_peer
            event["track_id"] = track["id"]
            event["chunk_size_bytes"] = random.choice([32768, 65536, 131072])
            event["latency_ms"] = random.randint(5, 120)
        elif event_type == "cache_hit":
            track = random.choice(SAMPLE_TRACKS)
            event["track_id"] = track["id"]
            event["cache_age_ms"] = random.randint(1000, 300000)
        elif event_type == "cache_miss":
            target_peer = random.choice([p for p in self.active_peers if p != peer_id] or self.active_peers)
            track = random.choice(SAMPLE_TRACKS)
            event["target_peer"] = target_peer
            event["track_id"] = track["id"]
            event["download_source"] = "p2p"
            event["latency_ms"] = random.randint(10, 200)

        return event

    # ── Publication ──────────────────────────────────────────

    def _publish_event(self, topic_key: str, event: dict):
        """Publie un événement dans Redis et (Phase 2) dans Kafka."""
        payload = json.dumps(event)
        channel = TOPICS[topic_key]

        self._publish_to_redis(channel, payload)
        self._publish_to_kafka(channel, event.get("user_id") or event.get("peer_id", ""), payload)

    def _publish_to_redis(self, channel: str, payload: str):
        """Publie payload dans le channel Redis. Tente une reconnexion automatique si la connexion est perdue."""
        try:
            self.redis.publish(channel, payload)
            self.redis_available = True
        except Exception as e:
            logger.warning(f"Redis: erreur sur {channel} ({e}) — tentative de reconnexion...")
            try:
                self.redis = redis.from_url(REDIS_URL, decode_responses=True, socket_keepalive=True)
                self.redis.publish(channel, payload)
                self.redis_available = True
                logger.info("Redis: reconnexion réussie.")
            except Exception as e2:
                self.redis_available = False
                logger.error(f"Redis: reconnexion échouée ({e2}), événement ignoré.")

    def _publish_to_kafka(self, topic: str, key: str, payload: str):
        """Publie payload dans le topic Kafka. La clé (user_id/peer_id) assure le partitionnement."""
        def _delivery_report(err, msg):
            if err:
                logger.error(f"Kafka delivery failed [{topic}] : {err}")

        try:
            self.kafka_producer.produce(
                topic,
                key=key.encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=_delivery_report,
            )
            self.kafka_producer.poll(0)
        except Exception as e:
            logger.error(f"Erreur Kafka sur {topic} : {e}")

    def _shutdown(self, signum, frame):
        logger.info(f"Arrêt du simulateur (signal {signum}) — {self.event_count} événements publiés")
        self.running = False
        self.kafka_producer.flush(timeout=10)


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SPOTIFY P2P Simulator")
    parser.add_argument("--peers",  type=int,   default=10,     help="Nombre de peers simulés")
    parser.add_argument("--rate",   type=float, default=5.0,    help="Événements par seconde")
    parser.add_argument("--mode",   type=str,   default="normal",
                        choices=["normal", "fraud", "late_events", "chaos"],
                        help="Mode de simulation")
    args = parser.parse_args()

    simulator = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode,
    )
    simulator.run()


if __name__ == "__main__":
    main()
