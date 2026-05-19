# Bronze Layer (Medallion Architecture)

This folder contains the data engineering pipeline responsible for the Bronze layer, focusing on ingestion, schema enforcement, and storing raw data from our high-volume and low-volume sources.

## 📂 Components Included

* **`Bronze_Layer.ipynb`**: Fabric Notebook utilizing PySpark to ingest raw data, handle incremental loading, and write data into Delta Lake format.
* **`Bronze_Database.sqlproj`**: The SQL Database Project containing the baseline tables and raw schemas for the Bronze data platform.
* **`xmla.json`**: Metadata configuration and semantic properties for data lineage and connectivity.

## 🚀 Key Responsibilities

1. **Raw Data Ingestion**: Captures full and incremental historical snapshots from source systems without heavy transformations.
2. **Delta Lake Storage**: Saves data in append-only Delta tables to maintain full historical data fidelity and auditability.
3. **Foundation for Silver**: Organizes the structure to allow efficient cleansing, deduplication, and transformation in the next stage (Silver layer).
