import json
import time
import pandas as pd
from kafka import KafkaProducer

# Initialize Kafka Producer pointing to your local Docker container
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'btc-trades'
file_path = "BTCUSDT-trades-2026-11-05.csv"

print("Starting trade stream producer...")

# Read CSV in chunks to maintain low memory usage
chunk_size = 1000
for chunk in pd.read_csv(file_path, header=None, chunksize=chunk_size):
    for _, row in chunk.iterrows():
        trade_data = {
            "trade_id": row[0],
            "price": row[1],
            "qty": row[2],
            "quote_qty": row[3],
            "time": row[4],
            "is_buyer_maker": row[5],
            "is_best_match": row[6]
        }
        
        # Send message to Kafka topic
        producer.send(TOPIC_NAME, value=trade_data)
        
    producer.flush()
    print(f"Sent batch of {chunk_size} trades to Kafka...")
    time.sleep(0.5)  # Controls stream pacing speed