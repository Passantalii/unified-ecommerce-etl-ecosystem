# Process Execution Sequence: End-to-End Data Flow

This diagram illustrates the functional orchestration of the data pipeline. It highlights how **PySpark Notebooks** act as the central engine, pulling data from various sources and moving it through the Medallion layers within **Microsoft Fabric**.

## 1. Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    %% Define Participants
    participant CSV as Historical CSV Data
    participant SMP as Random Samples
    participant NB as PySpark Notebook
    participant Br as Bronze Layer (Raw)
    participant Sl as Silver Layer (Cleaned)
    participant Gd as Gold Layer (Curated)
    participant SQL as SQL Endpoint
    participant AI as AI Agent / Power BI

    Note over CSV, NB: Step 1: Ingestion Phase
    CSV->>NB: Load Batch Data
    SMP->>NB: Load Streaming Samples
    NB->>Br: Write as Delta Tables (Raw)
    
    Note over Br, Sl: Step 2: Silver Transformation
    Br->>NB: Fetch Raw Data
    activate NB
    NB->>NB: Cleaning & Schema Enforcement
    NB->>Sl: Save Refined Tables
    deactivate NB
    
    Note over Sl, Gd: Step 3: Gold Aggregation
    Sl->>NB: Fetch Cleaned Data
    activate NB
    NB->>NB: Business Logic & Star Schema
    NB->>Gd: Save Final Dimensions/Facts
    deactivate NB

    Note over Gd, AI: Step 4: Serving Phase
    Gd->>SQL: Expose via SQL Endpoint
    SQL->>AI: Connect for Insights & ML
```
