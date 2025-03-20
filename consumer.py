from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "raw_smartwatch_data",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for message in consumer:
    data = message.value
    if data["heart_rate"]:
        alert = {
            "user_id": data["user_id"],
            "alert_type": "High Heart Rate",
            "heart_rate": data["heart_rate"],
            "timestamp": data["timestamp"]
        }
        print(f"ALERT: {alert}")
