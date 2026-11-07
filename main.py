import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer

app = FastAPI()

# Enable CORS so your future frontend can easily connect to it
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper function to initialize Kafka Consumer
def create_consumer():
    return KafkaConsumer(
        'btc-trades',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='fastapi-dashboard-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

@app.websocket("/ws/trades")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("Frontend client connected to WebSocket!")
    
    # Run consumer in a background task or thread-safe manner, or simple iteration
    consumer = create_consumer()
    try:
        for message in consumer:
            trade_data = message.value
            # Send the live trade JSON to the frontend dashboard
            await websocket.send_json(trade_data)
            # Yield control briefly to keep the async loop smooth
            await asyncio.sleep(0.001)
    except WebSocketDisconnect:
        print("Client disconnected.")
        consumer.close()
    except Exception as e:
        print(f"Error: {e}")
        consumer.close()