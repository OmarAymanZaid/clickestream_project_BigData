from kafka import KafkaConsumer
import json


# --- CONFIGURATION -------------------------------------------------------------------
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'clean_clickstream'
# -------------------------------------------------------------------------------------

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    group_id="nifi-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for msg in consumer:
    print("Received:", msg.value)
