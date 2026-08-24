# Architecture Deep Dive — Medallion Architecture

## Overview

This project implements the **Medallion Architecture** (also known as the Delta Architecture or multi-hop architecture) on **Azure Databricks** with **Delta Lake** as the storage layer and **dbt** for SQL-based transformations.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Data Sources                                    │
│  Azure SQL Database (AdventureWorksLT)  ·  REST APIs  ·  CSV Files     │
└───────────────────────────────┬────────────────────────────────────────┘
                                │  Azure Data Factory (ADF)
                                │  ─ Copy Activity (incremental by ModifiedDate)
                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    ADLS Gen2 — Raw Landing Zone                        │
│  abfss://raw@<account>.dfs.core.windows.net/adventureworks/<table>/    │
│  Format: Parquet  ·  Partitioned by ingestion date                     │
└───────────────────────────────┬────────────────────────────────────────┘
                                │  Databricks Notebook (01_bronze_ingestion.py)
                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER  (/mnt/bronze/)                                       │
│  Delta tables — exact copies of source with _loaded_at metadata        │
│  Schema: bronze  ·  Materialization: view (dbt) / table (PySpark)      │
│                                                                        │
│  bronze.customer  ·  bronze.product  ·  bronze.salesorderheader        │
│  bronze.salesorderdetail  ·  bronze.address  ·  bronze.productmodel    │
└───────────────────────────────┬────────────────────────────────────────┘
                                │  dbt (silver models) or
                                │  Databricks Notebook (02_silver_transformation.py)
                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER  (/mnt/silver/)                                       │
│  Cleaned · Deduplicated · Type-cast · Standardised                     │
│  Schema: silver  ·  Materialization: Delta table                       │
│                                                                        │
│  silver_customer  ·  silver_product  ·  silver_salesorder              │
│  + dbt Snapshots (SCD Type 2) for historical tracking                  │
└───────────────────────────────┬────────────────────────────────────────┘
                                │  dbt (gold models)
                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER  (/mnt/gold/)                                           │
│  Analytics-ready · Aggregated · Business logic applied                 │
│  Schema: gold  ·  Materialization: Delta table                         │
│                                                                        │
│  gold_sales_summary  ·  gold_customer_lifetime_value                   │
│  gold_product_performance  ·  dim_customer  ·  dim_product             │
└───────────────────────────────┬────────────────────────────────────────┘
                                │
                    ┌───────────┴────────────┐
                    ▼                        ▼
             Power BI Desktop         Databricks SQL
             (DirectQuery / Import)   (SQL Warehouse)
```

## Layer Responsibilities

### Bronze — Raw Ingestion

| Property       | Value                                      |
|----------------|--------------------------------------------|
| Purpose        | Land raw data with zero transformation     |
| Format         | Delta Lake (Parquet under the hood)        |
| Schema changes | Schema evolution enabled (`mergeSchema`)   |
| Materialization | dbt `view` (reads source directly)        |
| dbt tag        | `bronze`                                   |

Bronze models are intentionally lightweight. They add only a `_loaded_at` timestamp and expose all source columns. This gives us a complete, auditable record of every row that arrived from the source system.

### Silver — Cleaned & Standardised

| Property       | Value                                         |
|----------------|-----------------------------------------------|
| Purpose        | Apply data quality and business rules          |
| Format         | Delta Lake table                              |
| Key operations | Deduplication, null handling, type casting    |
| SCD support    | dbt snapshots produce SCD Type 2 history      |
| dbt tag        | `silver`                                      |

Silver models use `row_number()` window functions to deduplicate on the natural key, keeping only the latest `ModifiedDate` version of each record.

### Gold — Analytics-Ready

| Property       | Value                                            |
|----------------|--------------------------------------------------|
| Purpose        | Serve BI tools and data science models           |
| Format         | Delta Lake table                                 |
| Key operations | Aggregations, KPIs, dimensional modelling, RFM  |
| dbt tag        | `gold` / `mart`                                  |

Gold models implement business logic such as monthly revenue rollups, customer lifetime value (with RFM segmentation), and product performance metrics with gross margin calculations.

## dbt Model Lineage

```
Source (saleslt.*)
    └── bronze_customer
            └── silver_customer ──────────────────────────────┐
    └── bronze_product                                         │
            └── silver_product ──────────────────────────────┐│
    └── bronze_salesorderheader                               ││
    └── bronze_salesorderdetail                               ││
            └── silver_salesorder ───────────────────────────┤│
                        └── gold_sales_summary               ││
                        └── gold_customer_lifetime_value ─────┘│
                        └── gold_product_performance ──────────┘
```

## Delta Lake Key Features Used

| Feature                  | Layer   | Purpose                                         |
|--------------------------|---------|-------------------------------------------------|
| ACID transactions        | All     | Reliable concurrent reads/writes                |
| Time travel              | Silver  | Query data as of any previous timestamp         |
| Schema enforcement       | Bronze  | Prevent bad data from corrupting the table      |
| Schema evolution         | Bronze  | Accept new columns from the source              |
| Change Data Feed (CDF)   | Silver  | Incremental reads for downstream consumers      |
| Z-ORDER clustering       | Silver  | Faster queries on common join/filter columns    |
| Auto Optimize            | All     | Automatic small-file compaction                 |
| VACUUM                   | All     | Remove files outside retention window           |

## SCD Type 2 with dbt Snapshots

Snapshots in the `snapshots/` directory implement **Slowly Changing Dimension Type 2** using dbt's snapshot mechanism. When a source record changes, dbt:
1. Sets `dbt_valid_to` on the old record to the current timestamp.
2. Inserts a new row with `dbt_valid_to = null` (current version).

The Gold dimensional models filter on `dbt_valid_to is null` to select the current version of each record.

## Performance Considerations

- **Partitioning** is not applied at the Bronze/Silver layers for AdventureWorksLT due to small data volumes. For larger datasets, partition by `date_trunc('month', ModifiedDate)`.
- **Z-ORDER** is applied on foreign key columns (`CustomerID`, `ProductID`, `SalesOrderID`) to accelerate joins.
- **Auto Optimize** ensures files are compacted to the target 128 MB size without manual intervention.
- **Statistics** are collected via `ANALYZE TABLE … COMPUTE STATISTICS FOR ALL COLUMNS` to enable the Databricks Cost-Based Optimizer (CBO).
