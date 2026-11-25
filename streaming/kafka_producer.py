import json
import time
import glob
import os
import pandas as pd
from kafka import KafkaProducer

# 1. Connect to your Docker Kafka Broker (exposed on port 29092)
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = "solar-telemetry-stream"
#DATA_DIR = "/home/azhagesan/thesis/docker/processed_solar_data"
DATA_DIR = "/home/azhagesan/thesis/Solar-Digital-Twin-Framework/docker/processed_solar_data"


print(f"🚀 Starting Kafka Producer -> Streaming to topic: '{TOPIC_NAME}'")

# 2. Load your preprocessed Parquet records
parquet_files = glob.glob(os.path.join(DATA_DIR, "plant_id=*", "*.parquet"))
#if not parquet_files:
#    raise FileNotFoundError("No Parquet files found! Check DATA_DIR path.")

#df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
#df = df.sort_values(by="timestamp").head(100) # Stream first 100 records for POC
if not os.path.exists(DATA_DIR):
    raise FileNotFoundError("No Parquet directory found! Check DATA_DIR path.")

df = pd.read_parquet(DATA_DIR)
df = df.sort_values(by="timestamp").head(100) # Stream first 100 records for POC
# 3. Stream records in near-real-time
for index, row in df.iterrows():
    payload = {
        "timestamp": str(row["timestamp"]),
        "plant_id": int(row["plant_id"]),
        "ghi_w_m2": round(float(row["ghi_w_m2"]), 4),
        "temp_c": round(float(row["temp_c"]), 4),
        "expected_power_kw": round(float(row["expected_power_output_kw"]), 4),
        "actual_power_kw": round(float(row["actual_power_output_kw"]), 4),
        "deviation_ratio": round(float(row["power_deviation_ratio"]), 4)
    }
    
    producer.send(TOPIC_NAME, value=payload)
    print(f"[SENT] Plant {payload['plant_id']} | Actual: {payload['actual_power_kw']} | Dev: {payload['deviation_ratio']}")
    time.sleep(1) # Simulate 1-second SCADA sensor interval

producer.flush()
print("✅ Kafka POC Stream Completed.")
