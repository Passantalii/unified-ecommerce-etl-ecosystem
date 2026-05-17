# Low-Volume Data Ingestion Layer (Real-Time Streaming)

This directory documents the ingestion infrastructure for low-volume, high-frequency streaming data into our e-commerce analytics platform. We utilize **PySpark**, **Azure Event Hubs**, and **Microsoft Fabric Eventstreams** to simulate and capture live customer behavior data.

---

## 🛠️ Infrastructure Component Architecture
1. **Data Generator:** A PySpark Notebook uses the `Faker` library to mock streaming customer interactions.
2. **Message Broker:** Data is streamed continuously into an **Azure Event Hub** instance using asynchronous Python clients.
3. **Fabric Eventstream (`cust_stream`):** Connects to the Event Hub as a source and continuously routes the incoming live data directly into the Lakehouse Delta tables (**Bronze Layer**).

---

## 💻 Technical Implementation

### 1. PySpark Live Data Simulator (`Generate Streaming Data`)
This script initializes the streaming simulation, establishes a secure connection via connection strings, and defines configuration parameters for real-time traffic mocking.

```python
# Libraries utilized for core data streaming logic
import json
import random
import asyncio
from datetime import datetime, date
from faker import Faker
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

# Initialize Data Generator
fake = Faker()

# Event Hub Connection Configuration
CONNECTION_STR = "Endpoint=sb://esehsec1udzbw197hovt1cv.servicebus.windows.net/;SharedAccessKeyName=..."
EVENT_HUB_NAME = "es_9efb865f-8ea3-4430-b25f-5712d5252f2c"

# Stream Workload Configuration Boundaries
NUM_EXISTING_CUSTOMER_ORDERS = 500
NUM_NEW_CUSTOMER_ORDERS = 100
NUM_CUSTOMER_UPDATES = 30
NUM_PRICE_CHANGES = 3
