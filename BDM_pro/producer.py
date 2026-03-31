import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

cities = ["Chennai", "Delhi", "Mumbai", "Bangalore", "Hyderabad"]

print("Starting Advanced Weather Producer...")

while True:
    data = {
        "city": random.choice(cities),
        "temperature": random.randint(25, 50),
        "humidity": random.randint(30, 100),
        "wind_speed": random.randint(5, 30),
        "rainfall": random.randint(0, 25),
        "pressure": random.randint(980, 1050),
        "timestamp": int(time.time())   # seconds epoch (important for window)
    }

    producer.send('weather-topic', data)
    producer.flush()
    print("Sent:", data)
    time.sleep(2)