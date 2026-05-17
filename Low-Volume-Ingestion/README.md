# Low-Volume Data Ingestion Layer (Real-Time Streaming)

This directory documents the end-to-end real-time ingestion infrastructure for low-volume, high-frequency data streams within our e-commerce analytics platform. We utilize **PySpark**, **Azure Event Hubs**, and **Microsoft Fabric Eventstreams** to dynamically simulate, process, and capture live customer behavior events.

---

## 🛠️ Infrastructure & Data Flow Architecture

The live streaming data pipeline follows a decoupled architecture to ensure reliable data mocking and continuous flow:

1. **Live Data Simulator:** A PySpark Notebook utilizes the `Faker` library to continuously generate synthetic e-commerce transactions and state changes.
2. **Message Broker (Ingestion):** Payloads are serialized as JSON and streamed asynchronously to an **Azure Event Hub** instance using a Python producer client.
3. **Fabric Eventstream (`cust_stream`):** Acts as the real-time orchestrator that listens to the Event Hub source, routes the message queue, and performs low-latency ingestion directly into the Lakehouse Delta tables (**Bronze Layer**).
