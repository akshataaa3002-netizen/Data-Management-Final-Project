Markdown
# 🚀 Real-Time BTC/USDT Data Streaming Pipeline

A high-performance real-time data engineering pipeline built to ingest, process, and visualize high-frequency cryptocurrency trade data. This project simulates live order book flow by streaming millions of historical trade records through containerized Apache Kafka, delivering real-time price updates to an interactive web dashboard via WebSockets with sub-second latency.

---

## 🏗️ System Architecture

```text
+-----------------------+     +-------------------+     +-----------------+
|  Binance CSV Dataset  | --> |  Python Producer  | --> |  Apache Kafka   |
| (BTCUSDT-trades.csv)  |     |   (kafka-python)  |     |  (Docker Container)
+-----------------------+     +-------------------+     +-----------------+
                                                                |
                                                                v
+-----------------------+     +-------------------+     +-----------------+
|   Frontend Dashboard  | <-- | FastAPI Server    | <-- | Python Consumer |
|  (HTML5/JS WebSocket) |     |  (WebSockets API) |     | (Async Stream)  |
+-----------------------+     +-------------------+     +-----------------+
Tech Stack
Message Broker: Apache Kafka & Apache ZooKeeper (Docker containerized)

Data Ingestion & Processing: Python (pandas, kafka-python)

Backend API: FastAPI & Uvicorn (Asynchronous WebSockets)

Frontend UI: HTML5, CSS3

📂 Repository Structure
Plaintext
.
├── docker-compose.yml     # Infrastructure orchestration for ZooKeeper & Kafka
├── producer.py            # Stream ingestor reading CSV chunks into Kafka topic
├── consumer.py            # Kafka consumer service for stream validation
├── main.py                # FastAPI backend serving real-time WebSocket endpoints
├── index.html             # Dynamic web dashboard for live trade visualization
├── Prompts.docx           # Complete prompt logs and AI workflow history
└── README.md              # Project documentation
⚡ Getting Started
Prerequisites
Make sure you have the following installed on your machine:

Docker Desktop

Python 3.10+

VS Code (with the Live Server extension recommended)

Installation & Execution Guide
1. Set Up Python Environment
Install the required project dependencies:

Bash
pip install kafka-python pandas fastapi uvicorn
2. Download the Dataset
Obtain the historical trade data file (BTCUSDT-trades-2026-11-05.csv) and place it directly inside the project root folder.

3. Spin Up Kafka Infrastructure
Start the Kafka broker and ZooKeeper containers in detached mode:

Bash
docker-compose up -d
(Verify that both containers are healthy by running docker ps)

4. Launch the FastAPI WebSocket Server
Start the Uvicorn ASGI server to broadcast Kafka messages to frontend clients:

Bash
uvicorn main:app --reload
The backend service will initialize on http://127.0.0.1:8000.

5. Start the Stream Producer
In a separate terminal window, launch the producer to ingest and stream trades:

Bash
python producer.py
6. Open the Web Dashboard
Open index.html in your browser (or right-click in VS Code and select Open with Live Server). You will instantly see live trade streams dynamically rendering in the dashboard table!

⚙️ Key Technical Features
Chunked Ingestion: Reads multi-gigabyte datasets without memory overflow by using Pandas chunking mechanisms.

Asynchronous WebSocket Delivery: Replaces costly HTTP polling with persistent bi-directional WebSockets for minimal latency.

Front-End DOM Management: Automatically caps active table rows to prevent browser memory leaks during high-throughput streaming.

Order Depth Color Coding: Dynamically highlights buy side (green) vs. sell side (red) order flows in real time.
