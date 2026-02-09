"""
Kafka Consumer Script
Reads data from Kafka broker and logs to file
"""

import os
from json import loads
from kafka import KafkaConsumer

topic = "cot6930-felix"

# Create a consumer to read data from kafka
# Ref: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Experiment with different values (earliest reads all and latest reads only new messages)
    # Commit that an offset has been read
    enable_auto_commit=True,
    # How often to tell Kafka, an offset has been read
    auto_commit_interval_ms=1000
)

print('Reading Kafka Broker')
print('Press Ctrl+C to stop')
try:
    for message in consumer:
        message = message.value.decode()
        # Default message.value type is bytes!
        print(loads(message))
        os.system(f"echo {message} >> kafka_log.csv")
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    print("Consumer closed")
