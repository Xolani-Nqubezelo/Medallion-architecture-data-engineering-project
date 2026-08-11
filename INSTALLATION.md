# Installation Guide

## Quick Start (Local Development)

### Prerequisites

| Tool                    | Minimum Version | Notes                                    |
|-------------------------|-----------------|------------------------------------------|
| Python                  | 3.9+            | 3.11 recommended                         |
| pip                     | 23+             | `pip install --upgrade pip`              |
| Git                     | 2.x             |                                          |
| Azure CLI               | 2.50+           | For ADLS and Key Vault authentication    |
| Databricks CLI          | 0.200+          | `pip install databricks-cli`             |

---

## Step 1 — Clone the Repository

```bash
git clone https://github.com/Xolani-Nqubezelo/Medallion-architecture-data-engineering-project.git
cd Medallion-architecture-data-engineering-project
```

## Step 2 — Create a Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate         # Windows PowerShell
```

## Step 3 — Install Python Dependencies

```bash
pip install -r requirements.txt
```

## Step 4 — Configure dbt Profile

Create the file `~/.dbt/profiles.yml` (see [DATABRICKS_SETUP.md](DATABRICKS_SETUP.md#4-dbt-profile-configuration) for the full template):

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
```

## Step 5 — Set Environment Variables

```bash
export DBT_DATABRICKS_TOKEN="<your-personal-access-token>"
```

Add this to your shell profile (`~/.bashrc`, `~/.zshrc`) to persist it.

## Step 6 — Verify dbt Connection

```bash
dbt debug
```

Expected output: `All checks passed!`

## Step 7 — Run the Full Pipeline

```bash
# Bronze layer (views — instant)
dbt run --select tag:bronze

# Snapshots (SCD Type 2 — run before silver)
dbt snapshot

# Silver layer
dbt run --select tag:silver

# Gold layer
dbt run --select tag:gold

# Run all tests
dbt test

# Check source freshness
dbt source freshness
```

## Step 8 — Generate Documentation

```bash
dbt docs generate
dbt docs serve  # Opens http://localhost:8080 in your browser
```

---

## Docker Setup (Optional)

Build and run the project in a container for a fully reproducible environment:

```bash
docker build -t medallion-dbt .
docker run --rm \
  -e DBT_DATABRICKS_TOKEN=$DBT_DATABRICKS_TOKEN \
  -v ~/.dbt:/root/.dbt:ro \
  medallion-dbt \
  dbt run --select tag:gold
```

---

## Troubleshooting

### `dbt debug` fails with authentication error

Ensure your PAT has the `SQL access` permission in Databricks workspace settings.

### `dbt run` raises `SCHEMA_NOT_FOUND`

Run the Delta Lake setup notebook (`03_delta_lake_setup.py`) to create schemas, or run:

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

### Snapshots fail with `TABLE_OR_VIEW_NOT_FOUND`

Ensure the bronze ingestion has completed and the source tables exist in the `bronze` or `saleslt` schema.
