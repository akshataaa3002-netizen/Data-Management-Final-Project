import json
from kafka import KafkaConsumer

# Initialize Kafka Consumer listening to the btc-trades topic
consumer = KafkaConsumer(
    'btc-trades',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='btc-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for incoming trade streams...")
for message in consumer:
    trade = message.value
    print(f"Received Trade -> ID: {trade['trade_id']} | Price: ${trade['price']} | Qty: {trade['qty']}")