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
POLL_PEAK = int(os.getenv("POLL_INTERVAL_PEAK", "30"))           # 06:00-09:00, 15:00-18:30
POLL_SHOULDER = int(os.getenv("POLL_INTERVAL_SHOULDER", "60"))   # 09:00-15:00, 18:30-22:00
POLL_OFFPEAK = int(os.getenv("POLL_INTERVAL_OFFPEAK", "300"))   # 22:00-06:00
POLL_ALERTS = int(os.getenv("POLL_INTERVAL_ALERTS", "300"))      # service alerts — fixed


def _realtime_interval():
    """3-tier adaptive polling for vehicle_positions and trip_updates."""
    now = datetime.now()
    t = now.hour * 60 + now.minute  # minutes since midnight
    if (360 <= t < 540) or (900 <= t < 1110):  # 06:00-09:00, 15:00-18:30
        return POLL_PEAK
    elif (1320 <= t or t < 360):  # 22:00-06:00
        return POLL_OFFPEAK
    else:
        return POLL_SHOULDER

SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "schemas")


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
    informed = alert.get("informed_entity", [{}])
    route_id = informed[0].get("route_id") if informed else None
    periods = alert.get("active_period", [{}])
    period = periods[0] if periods else {}
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
        "timestamp": int(entity["timestamp"] if "timestamp" in entity else time.time()),
    }


FEEDS = {
    "at.vehicle_positions": {
        "url": f"{AT_BASE_URL}/vehiclelocations",
        "schema_file": "vehicle_position.avsc",
        "flatten": _flatten_vehicle_position,
        "key_field": "vehicle_id",
        "interval": _realtime_interval,  # adaptive
    },
    "at.trip_updates": {
        "url": f"{AT_BASE_URL}/tripupdates",
        "schema_file": "trip_update.avsc",
        "flatten": _flatten_trip_update,
        "key_field": "trip_id",
        "interval": _realtime_interval,  # adaptive
    },
    "at.service_alerts": {
        "url": f"{AT_BASE_URL}/servicealerts",
        "schema_file": "service_alert.avsc",
        "flatten": _flatten_service_alert,
        "key_field": "id",
        "interval": lambda: POLL_ALERTS,  # fixed 5 min
    },
}


# reuse TCP connections across polls — prevents ephemeral port exhaustion
# on long-running sessions (each requests.get() without a Session opens a
# new socket that sits in TIME_WAIT for 120s after close)
_session = requests.Session()
_session.headers["Ocp-Apim-Subscription-Key"] = AT_API_KEY


def fetch_entities(url):
    resp = _session.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()["response"]["entity"]


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for {msg.key()}: {err}")


def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    string_serializer = StringSerializer("utf_8")
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    serializers = {}
    for topic, cfg in FEEDS.items():
        schema_str = _load_schema(cfg["schema_file"])
        serializers[topic] = AvroSerializer(sr_client, schema_str)

    print(f"Starting AT producer → {len(FEEDS)} feeds, "
          f"peak={POLL_PEAK}s / shoulder={POLL_SHOULDER}s / offpeak={POLL_OFFPEAK}s / alerts={POLL_ALERTS}s")

    _running = True

    def _stop(sig, frame):
        nonlocal _running
        print(f"\nCaught signal {sig}, shutting down...")
        _running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    # per-feed last-poll tracking — poll all on first tick
    last_poll = {topic: 0.0 for topic in FEEDS}

    while _running:
        now = time.monotonic()
        polled_any = False

        for topic, cfg in FEEDS.items():
            interval = cfg["interval"]()
            if now - last_poll[topic] < interval:
                continue

            try:
                entities = fetch_entities(cfg["url"])
                for entity in entities:
                    record = cfg["flatten"](entity)
                    producer.produce(
                        topic=topic,
                        key=string_serializer(record[cfg["key_field"]]),
                        value=serializers[topic](
                            record,
                            SerializationContext(topic, MessageField.VALUE),
                        ),
                        on_delivery=delivery_report,
                    )
                print(f"  {topic}: {len(entities)} messages")
                polled_any = True
            except Exception as e:
                print(f"  {topic}: ERROR {e}")

            last_poll[topic] = time.monotonic()

        if polled_any:
            producer.flush()

        if _running:
            # sleep until the next feed is due
            now = time.monotonic()
            next_due = min(
                last_poll[t] + cfg["interval"]() - now
                for t, cfg in FEEDS.items()
            )
            sleep_for = max(1, next_due)
            time.sleep(sleep_for)

    producer.flush()
    print("Producer stopped cleanly")


if __name__ == "__main__":
    main()
