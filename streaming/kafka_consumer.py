import json
from kafka import KafkaConsumer

TOPIC_NAME = "solar-telemetry-stream"

# 1. Initialize Consumer connected to your Docker Kafka Broker
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='digital-twin-analytics-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"📡 Listening for live SCADA streams on '{TOPIC_NAME}'...\n")

# 2. Process incoming micro-events in real time
for message in consumer:
    data = message.value
    plant_id = data['plant_id']
    dev = data['deviation_ratio']
    
    status = "⚠️ ANOMALY DETECTED" if dev > 0.15 else "✅ NORMAL"
    print(f"[RECEIVED] Plant {plant_id} @ {data['timestamp']} | Deviation: {dev:.2f} -> {status}")
