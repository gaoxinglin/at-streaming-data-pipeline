"""
Shared Kafka helpers for local (Redpanda + Confluent SR) and cloud (Azure Event Hubs) modes.

Cloud mode is activated by setting EVENTHUBS_CONNECTION_STRING in the environment.
- SASL_SSL replaces the default plaintext connection
- Avro schemas are read from .avsc files instead of Schema Registry
- Confluent 5-byte wire-format header is absent; fastavro writes raw bytes
- Event Hubs entity names are bare (no "at." prefix)
"""
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

EVENTHUBS_CONNECTION_STRING = os.getenv("EVENTHUBS_CONNECTION_STRING", "")
CLOUD_MODE = bool(EVENTHUBS_CONNECTION_STRING)

_SCHEMA_FILES = {
    "at.vehicle_positions": "vehicle_position.avsc",
    "at.trip_updates": "trip_update.avsc",
    "at.service_alerts": "service_alert.avsc",
}
_SCHEMAS_DIR = Path(__file__).parent.parent / "ingestion" / "schemas"

# Confluent wire format prepends a 5-byte header (magic byte + 4-byte schema ID).
# fastavro in cloud mode emits raw bytes — no header to skip.
AVRO_VALUE_EXPR = "value" if CLOUD_MODE else "substring(value, 6)"


def topic_name(local_topic: str) -> str:
    """Map local 'at.*' topic name to the broker-side name."""
    return local_topic.removeprefix("at.") if CLOUD_MODE else local_topic


def load_schema(topic: str, schema_registry_url: str = "") -> str:
    """Return Avro schema JSON — from .avsc file in cloud mode, Schema Registry locally."""
    if CLOUD_MODE:
        return (_SCHEMAS_DIR / _SCHEMA_FILES[topic]).read_text()
    resp = requests.get(
        f"{schema_registry_url}/subjects/{topic}-value/versions/latest", timeout=10
    )
    resp.raise_for_status()
    return resp.json()["schema"]


def kafka_options(bootstrap: str) -> dict:
    """Spark Kafka source/sink options — adds SASL_SSL for Event Hubs in cloud mode."""
    opts = {"kafka.bootstrap.servers": bootstrap}
    if CLOUD_MODE:
        jaas = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="$ConnectionString" password="{EVENTHUBS_CONNECTION_STRING}";'
        )
        opts.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": jaas,
        })
    return opts
