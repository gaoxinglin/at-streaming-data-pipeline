"""
Spike: AT API → Redpanda (raw JSON, no Avro)
验证目标: Producer 能把 AT API 数据推进 Redpanda
"""
import json
import os

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

AT_API_KEY = os.getenv("AT_API_KEY")
AT_BASE_URL = os.getenv("AT_BASE_URL")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = "at.trip_updates"


def fetch_trip_updates() -> list:
    resp = requests.get(
        f"{AT_BASE_URL}/tripupdates",
        headers={"Ocp-Apim-Subscription-Key": AT_API_KEY},
        timeout=10,
    )
    resp.raise_for_status()
    entities = resp.json()["response"]["entity"]
    print(f"[API] 拿到 {len(entities)} 条 entity")
    return entities


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    entities = fetch_trip_updates()

    for entity in entities[:5]:
        producer.produce(TOPIC, value=json.dumps(entity).encode())
        print(f"[Kafka] 推送: {entity['id']}")

    producer.flush()
    print(f"\n✅ Spike 成功: 5 条消息已推入 topic '{TOPIC}'")
    print(f"   → 打开 http://localhost:8080 可以在 Redpanda Console 看到消息")


if __name__ == "__main__":
    main()
