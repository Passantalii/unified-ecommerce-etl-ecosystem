# High-Volume Ingestion Layer

This folder contains the data engineering components responsible for ingesting and processing high-volume raw e-commerce data into the Bronze layer of our data platform.

## 📂 Components Included

* **`HistoricalData.ipynb`**: The main Fabric Notebook containing the PySpark/Python logic to load, clean, and ingest large-scale historical data.
* **`HistLake.sqlproj`**: The SQL Database Project containing the schemas, tables, and views for the SQL analytics endpoint (`HistLake`).
* **`xmla.json`**: The dataset/semantic model properties and metadata configuration for connectivity.

## 🚀 Architecture Workflow

1.  **Ingestion**: Raw, high-volume source data is extracted and processed via the `HistoricalData` notebook.
2.  **Storage**: Data is stored efficiently in the **HistLake** Lakehouse (Bronze layer).
3.  **Analytics Endpoint**: The schema is exposed through the SQL analytics endpoint for downstream transformation and gold-layer serving.
