"""
Kafka Consumer Script
Reads data from Kafka broker and logs to file
"""

import os
from kafka import KafkaConsumer

topic = "cot6930-felix"

# Create a consumer to read data from kafka
# Ref: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=["localhost:19092"],
    auto_offset_reset="earliest",  # Experiment with different values (earliest reads all and latest reads only new messages)
    # Commit that an offset has been read
    enable_auto_commit=True,
    # How often to tell Kafka, an offset has been read
    auto_commit_interval_ms=1000,
    # SSL configuration to connect to the remote Kafka broker
    security_protocol="SSL",
    ssl_cafile="./certs/ca.pem",
    ssl_certfile="./certs/service.cert",
    ssl_keyfile="./certs/service.key",
    api_version=(4, 1, 1),
    ssl_check_hostname=False,
    request_timeout_ms=30000,
    metadata_max_age_ms=30000,
)

print("Reading Kafka Broker")
print("Press Ctrl+C to stop")
try:
    for message in consumer:
        # message is a ConsumerRecord object
        decoded_message = message.value.decode("utf-8")
        print(f"Offset: {message.offset} | Received: {decoded_message}")
        os.system(f"echo {decoded_message} >> kafka_log.csv")
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    print("Consumer closed")
