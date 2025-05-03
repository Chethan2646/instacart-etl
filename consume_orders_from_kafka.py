from kafka import KafkaConsumer
import json

# Create a new consumer group to avoid cached offsets
consumer = KafkaConsumer(
    'orders_stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='debug-consumer-6',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=15000  # Will exit after 5s if no messages
)

print("Connected to Kafka and subscribed to 'orders_stream'")
print("Waiting for messages...\n")

# Listen for messages
for message in consumer:
    print(f"Received message: {message.value}")
