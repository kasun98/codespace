from kafka import KafkaProducer
import json
import random
import time


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def generate_data():
    while True:
        for user_id in range(1, 10000):
            data = {
                "user_id": user_id,
                "device_id": random.randint(1, 100),
                "heart_rate": random.randint(60, 150),
                "steps": random.randint(0, 500),
                "calories_burned": round(random.uniform(0.1, 10.0), 2),
                "timestamp": int(time.time())
            }
            producer.send("raw_smartwatch_data", data)

            if user_id % 1000 == 0:  # Flush every 1000 messages
                producer.flush()
        
        print("Produced batch of 10,000 records")
        producer.flush()  # Ensure all messages are sent
        time.sleep(30)  # Wait 5 minutes

generate_data()

