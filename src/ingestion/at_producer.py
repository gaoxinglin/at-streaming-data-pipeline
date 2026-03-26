import json
import os
import signal
import time
from datetime import datetime

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from dotenv import load_dotenv

load_dotenv()

AT_API_KEY = os.environ["AT_API_KEY"]
AT_BASE_URL = os.getenv("AT_BASE_URL", "https://api.at.govt.nz/realtime/legacy")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
POLL_PEAK = int(os.getenv("POLL_INTERVAL_PEAK", "40"))       # 6:00-21:00
POLL_OFFPEAK = int(os.getenv("POLL_INTERVAL_OFFPEAK", "120"))  # 21:00-6:00


def _poll_interval():
    hour = datetime.now().hour
    return POLL_PEAK if 6 <= hour < 21 else POLL_OFFPEAK

SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "schemas")

# topic → (api path, schema file, key extractor, entity flattener)
FEEDS = {
    "at.vehicle_positions": {
        "url": f"{AT_BASE_URL}/vehiclelocations",
        "schema_file": "vehicle_position.avsc",
        "flatten": "_flatten_vehicle_position",
        "key_field": "vehicle_id",
    },
    "at.trip_updates": {
        "url": f"{AT_BASE_URL}/tripupdates",
        "schema_file": "trip_update.avsc",
        "flatten": "_flatten_trip_update",
        "key_field": "trip_id",
    },
    "at.service_alerts": {
        "url": f"{AT_BASE_URL}/servicealerts",
        "schema_file": "service_alert.avsc",
        "flatten": "_flatten_service_alert",
        "key_field": "id",
    },
}


def _load_schema(filename):
    with open(os.path.join(SCHEMAS_DIR, filename)) as f:
        return f.read()


def _flatten_vehicle_position(entity):
    v = entity["vehicle"]
    pos = v["position"]
    trip = v.get("trip", {})
    speed = pos.get("speed")
    bearing = pos.get("bearing")
    return {
        "vehicle_id": entity["id"],
        "trip_id": trip.get("trip_id", ""),
        "route_id": trip.get("route_id", ""),
        "latitude": pos["latitude"],
        "longitude": pos["longitude"],
        "bearing": float(bearing) if bearing is not None else None,
        "speed": float(speed) if speed is not None else None,
        "current_stop_sequence": v.get("current_stop_sequence"),
        "stop_id": v.get("stop_id"),
        "current_status": v.get("current_status"),
        "congestion_level": v.get("congestion_level"),
        "occupancy_status": str(v["occupancy_status"]) if "occupancy_status" in v else None,
        "timestamp": int(v["timestamp"]),
    }


def _flatten_trip_update(entity):
    tu = entity["trip_update"]
    trip = tu["trip"]
    return {
        "id": entity["id"],
        "trip_id": trip["trip_id"],
        "route_id": trip.get("route_id", ""),
        "direction_id": trip.get("direction_id"),
        "start_time": trip.get("start_time"),
        "start_date": trip.get("start_date"),
        "schedule_relationship": trip.get("schedule_relationship"),
        "delay": tu.get("delay", 0),
        "timestamp": int(tu["timestamp"]),
        "is_deleted": entity.get("is_deleted", False),
    }


def _flatten_service_alert(entity):
    alert = entity["alert"]
    # route_id from first informed_entity if available
    informed = alert.get("informed_entity", [{}])
    route_id = informed[0].get("route_id") if informed else None
    # active periods
    periods = alert.get("active_period", [{}])
    period = periods[0] if periods else {}
    # text fields
    header = alert.get("header_text", {})
    desc = alert.get("description_text", {})
    return {
        "id": entity["id"],
        "route_id": route_id,
        "cause": alert.get("cause"),
        "effect": alert.get("effect"),
        "header_text": header.get("translation", [{}])[0].get("text") if header else None,
        "description_text": desc.get("translation", [{}])[0].get("text") if desc else None,
        "active_period_start": period.get("start"),
        "active_period_end": period.get("end"),
        "timestamp": int(entity.get("timestamp", 0) or time.time()),
    }


FLATTEN_FNS = {
    "_flatten_vehicle_position": _flatten_vehicle_position,
    "_flatten_trip_update": _flatten_trip_update,
    "_flatten_service_alert": _flatten_service_alert,
}


def fetch_entities(url):
    headers = {"Ocp-Apim-Subscription-Key": AT_API_KEY}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()["response"]["entity"]


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for {msg.key()}: {err}")


def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    string_serializer = StringSerializer("utf_8")
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    # build serializers from .avsc files
    serializers = {}
    for topic, cfg in FEEDS.items():
        schema_str = _load_schema(cfg["schema_file"])
        serializers[topic] = AvroSerializer(sr_client, schema_str)

    print(f"Starting AT producer → {len(FEEDS)} feeds, peak={POLL_PEAK}s / offpeak={POLL_OFFPEAK}s")

    _running = True

    def _stop(sig, frame):
        nonlocal _running
        print(f"\nCaught signal {sig}, shutting down...")
        _running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    while _running:
        for topic, cfg in FEEDS.items():
            try:
                entities = fetch_entities(cfg["url"])
                flatten_fn = FLATTEN_FNS[cfg["flatten"]]

                for entity in entities:
                    record = flatten_fn(entity)
                    producer.produce(
                        topic=topic,
                        key=string_serializer(record[cfg["key_field"]]),
                        value=serializers[topic](
                            record,
                            SerializationContext(topic, MessageField.VALUE),
                        ),
                        on_delivery=delivery_report,
                    )

                producer.flush()
                print(f"  {topic}: {len(entities)} messages")

            except Exception as e:
                print(f"  {topic}: ERROR {e}")

        if _running:
            interval = _poll_interval()
            print(f"  next poll in {interval}s")
            time.sleep(interval)

    producer.flush()
    print("Producer stopped cleanly")


if __name__ == "__main__":
    main()
