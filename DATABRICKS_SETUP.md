# Databricks Setup Guide

## Prerequisites

- Azure subscription with Owner or Contributor role
- Azure Databricks workspace (Premium tier recommended for Unity Catalog)
- Azure Data Lake Storage Gen2 account
- Azure Data Factory instance
- Service Principal with Storage Blob Data Contributor role on ADLS

---

## 1. Workspace Configuration

### 1.1 Create a Databricks Cluster

Use the following cluster configuration for development:

```
Runtime:        14.3 LTS (Spark 3.5, Scala 2.12)
Node type:      Standard_DS3_v2  (14 GB RAM, 4 cores)
Worker count:   2–8 (auto-scaling)
Spot instances: Enabled (reduces cost by ~70 %)
```

For production, use a Job Cluster triggered by Databricks Workflows to avoid idle compute costs.

### 1.2 Cluster Libraries

Install the following Maven library on the cluster:

```
com.microsoft.azure:spark-mssql-connector_2.12:1.3.0-BETA
```

And these PyPI packages:

```
dbt-databricks>=1.7.0
databricks-sdk>=0.20.0
```

---

## 2. ADLS Gen2 Mount Points

Run this in a Databricks notebook to mount ADLS containers:

```python
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id":
        dbutils.secrets.get(scope="kv-scope", key="sp-client-id"),
    "fs.azure.account.oauth2.client.secret":
        dbutils.secrets.get(scope="kv-scope", key="sp-client-secret"),
    "fs.azure.account.oauth2.client.endpoint":
        f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='kv-scope', key='tenant-id')}/oauth2/token",
}

for layer in ["raw", "bronze", "silver", "gold"]:
    dbutils.fs.mount(
        source=f"abfss://{layer}@<your-adls-account>.dfs.core.windows.net/",
        mount_point=f"/mnt/{layer}",
        extra_configs=configs,
    )
    print(f"Mounted /mnt/{layer}")
```

---

## 3. Databricks Secrets (Key Vault backed)

Store sensitive credentials in Azure Key Vault and reference them via Databricks Secret Scopes.

```bash
# Create a secret scope backed by Azure Key Vault
databricks secrets create-scope \
  --scope kv-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<kv-name> \
  --dns-name https://<kv-name>.vault.azure.net/
```

Secrets to store in Key Vault:

| Key                    | Description                              |
|------------------------|------------------------------------------|
| `sp-client-id`         | Service Principal Application (client) ID |
| `sp-client-secret`     | Service Principal client secret          |
| `tenant-id`            | Azure AD Tenant ID                       |
| `sql-connection-string`| Azure SQL DB connection string           |

---

## 4. dbt Profile Configuration

Create `~/.dbt/profiles.yml`:

```yaml
medallion_dbt_spark:
  target: dev
  outputs:
    dev:
      type: databricks
      schema: dev_medallion
      host: "<your-workspace>.azuredatabricks.net"
      http_path: "/sql/1.0/warehouses/<warehouse-id>"
      token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
      threads: 4

    prod:
      type: databricks
      schema: medallion
      host: "<your-workspace>.azuredatabricks.net"
      http_path: "/sql/1.0/warehouses/<warehouse-id>"
      token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
      threads: 8
```

Set the token as an environment variable:

```bash
export DBT_DATABRICKS_TOKEN=$(databricks auth token --host https://<workspace>.azuredatabricks.net)
```

---

## 5. Azure Data Factory Pipeline

### 5.1 Source Connection

Create a **Linked Service** for Azure SQL Database:

- Type: `AzureSqlDatabase`
- Server: `<server>.database.windows.net`
- Database: `AdventureWorksLT`
- Authentication: SQL Authentication / Managed Identity

### 5.2 Sink Connection

Create a **Linked Service** for ADLS Gen2:

- Type: `AzureDataLakeStorageGen2`
- Account: `<your-adls-account>`
- Authentication: Service Principal

### 5.3 Pipeline Design

```
ForEach (table list)
  └── Copy Activity
        Source:  Azure SQL Database  (query: SELECT * FROM saleslt.<table> WHERE ModifiedDate >= @{pipeline().parameters.watermark})
        Sink:    ADLS Gen2 Parquet   (path: raw/adventureworks/<table>/year=@{formatDateTime(utcnow(),'yyyy')}/...)
        Settings: Enable staging = No, parallelCopies = 4
```

### 5.4 Trigger

Schedule trigger: daily at 02:00 UTC

---

## 6. Databricks Workflow (Orchestration)

Create a Databricks Workflow with the following tasks:

```
Task 1: bronze_ingestion
  Notebook: /notebooks/01_bronze_ingestion
  Cluster:  Job cluster (Standard_DS3_v2, 2 workers)

Task 2: dbt_silver  [depends_on: bronze_ingestion]
  Type: dbt task
  Commands: dbt run --select tag:silver
  Warehouse: SQL Warehouse

Task 3: dbt_snapshots  [depends_on: dbt_silver]
  Type: dbt task
  Commands: dbt snapshot

Task 4: dbt_gold  [depends_on: dbt_snapshots]
  Type: dbt task
  Commands: dbt run --select tag:gold

Task 5: dbt_test  [depends_on: dbt_gold]
  Type: dbt task
  Commands: dbt test

Task 6: delta_optimise  [depends_on: dbt_test]
  Notebook: /notebooks/03_delta_lake_setup
  Cluster:  Job cluster
```

---

## 7. Unity Catalog (Optional — Recommended for Production)

If your workspace has Unity Catalog enabled, update your dbt profile to use a three-part namespace:

```yaml
catalog: medallion_catalog  # Unity Catalog catalog name
schema: gold
```

And create the catalog/schemas:

```sql
CREATE CATALOG IF NOT EXISTS medallion_catalog;
CREATE SCHEMA IF NOT EXISTS medallion_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS medallion_catalog.silver;
CREATE SCHEMA IF NOT EXISTS medallion_catalog.gold;
```

---

## 8. Monitoring & Alerting

- Enable **Databricks SQL Query History** to track slow queries.
- Set up **Azure Monitor** alerts on ADF pipeline failures.
- Use `dbt source freshness` in the workflow to alert on stale data.
- Configure **Delta Lake table metrics** in Azure Monitor via Databricks REST API.
