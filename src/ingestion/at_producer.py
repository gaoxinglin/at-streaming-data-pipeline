import os
import time

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from dotenv import load_dotenv

load_dotenv()

AT_API_KEY = os.environ["AT_API_KEY"]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = "at.vehicle_positions"
POLL_INTERVAL = 30

AT_API_URL = "https://api.at.govt.nz/realtime/legacy/vehiclelocations"

VEHICLE_POSITION_SCHEMA = """
{
  "type": "record",
  "name": "VehiclePosition",
  "namespace": "nz.at",
  "fields": [
    {"name": "vehicle_id",  "type": "string"},
    {"name": "latitude",    "type": "double"},
    {"name": "longitude",   "type": "double"},
    {"name": "speed",       "type": ["null", "float"], "default": null},
    {"name": "timestamp",   "type": "long"},
    {"name": "is_deleted",  "type": "boolean", "default": false}
  ]
}
"""


def fetch_vehicles() -> list[dict]:
    headers = {"Ocp-Apim-Subscription-Key": AT_API_KEY}
    response = requests.get(AT_API_URL, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()["response"]["entity"]


def entity_to_record(entity: dict) -> dict:
    v = entity["vehicle"]
    pos = v["position"]
    speed = pos.get("speed")
    return {
        "vehicle_id": entity["id"],
        "latitude": pos["latitude"],
        "longitude": pos["longitude"],
        "speed": float(speed) if speed is not None else None,
        "timestamp": int(v["timestamp"]),
        "is_deleted": entity.get("is_deleted", False),
    }


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for {msg.key()}: {err}")


def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr_client, VEHICLE_POSITION_SCHEMA)
    string_serializer = StringSerializer("utf_8")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    print(f"Starting AT producer → topic={TOPIC}, poll every {POLL_INTERVAL}s")

    while True:
        try:
            entities = fetch_vehicles()
            print(f"Fetched {len(entities)} vehicles")

            for entity in entities:
                record = entity_to_record(entity)
                producer.produce(
                    topic=TOPIC,
                    key=string_serializer(record["vehicle_id"]),
                    value=avro_serializer(
                        record,
                        SerializationContext(TOPIC, MessageField.VALUE),
                    ),
                    on_delivery=delivery_report,
                )

            producer.flush()
            print(f"Produced {len(entities)} messages to {TOPIC}")

        except Exception as e:
            print(f"Error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
