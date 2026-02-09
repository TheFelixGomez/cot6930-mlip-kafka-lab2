"""
Kafka Producer Script
Writes data to Kafka broker
"""

from datetime import datetime
from json import dumps
from time import sleep
from random import randint
from kafka import KafkaProducer

# Update this for your own recitation section :)
topic = "cot6930-felix"

# Create a producer to write data to kafka
# Ref: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: dumps(x).encode("utf-8"),
)

# Add cities of your choice
cities = ["Bogota", "Boca Raton", "London", "Tokyo", "New York", "Sydney"]

# Write data via the producer
print("Writing to Kafka Broker")
for i in range(10):
    data = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{cities[randint(0, len(cities) - 1)]},{randint(18, 32)}ºC"
    print(f"Writing: {data}")
    producer.send(topic=topic, value=data)
    sleep(1)

print("Finished writing 10 messages to Kafka")
producer.close()