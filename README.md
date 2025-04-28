# Incremental Data Pipeline with Medallion Architecture

---

## Table of Contents

- [Project Description](#project-description)
- [Features](#features)
- [Setup](#setup)
- [Landing Zone and External Locations](#landing-zone-and-external-locations)
- [Data Processing Pipeline](#data-processing-pipeline)
  - [Bronze Layer](#bronze-layer)
  - [Silver Layer](#silver-layer)
  - [Gold Layer](#gold-layer)
- [Medallion Architecture](#medallion-architecture)
- [Checkpointing and Incremental Loading](#checkpointing-and-incremental-loading)
- [Function Reference](#function-reference)
- [Usage](#usage)

---

## Project Description

This project implements an **Incremental Data Pipeline** on **Databricks**, utilizing **Medallion Architecture** principles and **Unity Catalog** for centralized governance.

The pipeline is organized into three key layers:

- **Bronze Layer**: Ingests raw data directly from external sources without transformations.
- **Silver Layer**: Processes, cleanses, and enriches the raw data to create structured, analysis-ready datasets.
- **Gold Layer**: Aggregates and transforms the enriched data for final consumption by business intelligence and reporting systems.

In addition to core data processing, the project establishes a robust infrastructure that includes:

- **Landing Zone**: A secure area for initial data ingestion.
- **External Locations**: Configured storage paths for scalable and governed data storage.
- **Unity Metastore**: Centralized metadata management.
- **Unity Catalog**: Fine-grained access control and data governance.

This pipeline design ensures a scalable, secure, and efficient framework for real-time and batch data processing in the cloud.


---

## Features

- **Landing Zone** setup for secure data intake.
- **External Locations** for scalable storage paths.
- **Medallion Architecture** with Bronze, Silver, Gold layers.
- **Structured Streaming** with Delta Lake tables.
- **Checkpointing** for incremental loading.
- **Unity Catalog** for centralized metadata and access control.
- **Modular Functions** for easy pipeline extension.

---

## Setup

<img src="Project_screenshots/Screenshot 2025-04-28 232156.png" width="600"/>

1. **Unity Metastore**: Create and configure Unity Metastore.
2. **External Locations**: Register storage paths.
3. **Databricks Environment**: Enable Unity Catalog, Delta Lake, Structured Streaming.
4. **Cluster Setup**: Install required libraries (e.g., Databricks Runtime 12.2+).

---

## Landing Zone and External Locations

- Landing Zone acts as the initial raw data ingestion point.
- External Locations are configured with access controls to connect cloud storage to Databricks securely.

Key steps:

- Create storage credentials.
- Assign storage paths for Bronze, Silver, and Gold.
- Secure access using Unity Catalog permissions.


---

## Data Processing Pipeline

The pipeline follows a layered approach for better manageability and performance.

### Bronze Layer

- Raw data ingestion from external sources.
- Minimal transformations; store data as-is in Delta format.
- Set up checkpoint directories to support streaming.

---

### Silver Layer

- Cleansing operations:
  - Remove duplicates
  - Handle missing/null values
  - Standardize column names
- Enrichment:
  - Add calculated fields like `VEHICLE_INTENSITY` and `LOAD_TIME`.
---

### Gold Layer

- Business-specific aggregations and summarizations.
- Prepares analytical datasets for BI tools and reporting.

Examples:

- Group by time periods, locations, and vehicle types.
- Compute aggregated metrics like average load time.

---

## Medallion Architecture

The project adheres to the **Medallion Architecture** design:

| Layer  | Purpose                            |
| ------ | ---------------------------------- |
| Bronze | Raw ingested data.                  |
| Silver | Cleaned and enriched structured data. |
| Gold   | Aggregated and business-ready data.   |

Benefits:

- Clear separation of concerns.
- Scalability and maintainability.
- Easy incremental updates.
<p float="left">
  <img src="Project_screenshots/Screenshot 2025-04-28 232221.png" width="500"/>
  <img src="Project_screenshots/Screenshot 2025-04-28 232310.png" width="500"/>
</p>
---

## Checkpointing and Incremental Loading

- **Checkpointing** is implemented using Delta Lake's Structured Streaming features.
- Checkpoints track the last processed batch.
- Ensures idempotency and supports fault tolerance.

Key components:

- Stream read options include `checkpointLocation`.
- Data reprocessing is avoided on pipeline reruns.

---

## Function Reference

| Function Name              | Purpose                                     |
| -----------------------    |------------------------------------------- |
| `bronze_external_location` | Ingest raw data from external locations.     |
| `silver_external_location` | Cleanse and enrich Bronze data into Silver.  |
| `gold_external_location` | Aggregate Silver data into Gold datasets.    |
| `checkpoint_external_location` | Manage checkpoint directories for streaming. |
| `landing_external_location` | Creates external locations to store full raw data dumps in the Landing Zone. |
---
<p float="left">
  <img src="Project_screenshots/Screenshot 2025-04-28 232449.png" width="500"/>
  <img src="Project_screenshots/Screenshot 2025-04-28 232431.png" width="500"/>
</p>

<p float="left">
  <img src="Project_screenshots/Screenshot 2025-04-28 235104.png" width="500"/>
  <img src="Project_screenshots/Screenshot 2025-04-29 002026.png" width="480"/>
</p>

## Usage

1. **Run Landing Zone Setup**:
   - Configure External Locations and Storage Credentials.

2. **Execute Bronze Layer Ingestion**:
   - Start raw data streaming ingestion jobs.

3. **Process Silver Layer**:
   - Clean and enrich Bronze layer data.

4. **Aggregate Gold Layer**:
   - Apply business logic and generate final reports.

5. **Monitor Streaming Jobs**:
   - Check checkpoint locations to ensure incremental loads.

6. **Validate Data Across Layers**:
   - Compare layer outputs to ensure correct transformations.

---

# End of Document

