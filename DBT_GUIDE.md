# dbt Guide — Medallion Architecture

## Project Overview

This dbt project (`medallion_dbt_spark`) transforms raw AdventureWorksLT data through three layers:

| Layer   | dbt Tag     | Materialization | Purpose                              |
|---------|-------------|-----------------|--------------------------------------|
| Bronze  | `bronze`    | view            | Raw source exposure with metadata    |
| Silver  | `silver`    | Delta table     | Cleaned, deduplicated, typed         |
| Gold    | `gold`/`mart`| Delta table    | Analytics-ready aggregates and dims  |

---

## Directory Structure

```
medallion_dbt_spark/
├── dbt_project.yml          # Project configuration
├── models/
│   ├── bronze/              # Bronze views (source exposure)
│   │   ├── bronze_sources.yml
│   │   ├── bronze_models.yml
│   │   ├── bronze_customer.sql
│   │   ├── bronze_product.sql
│   │   ├── bronze_salesorderheader.sql
│   │   ├── bronze_salesorderdetail.sql
│   │   ├── bronze_address.sql
│   │   ├── bronze_customeraddress.sql
│   │   └── bronze_productmodel.sql
│   ├── silver/              # Silver cleaned tables
│   │   ├── silver_models.yml
│   │   ├── silver_customer.sql
│   │   ├── silver_product.sql
│   │   └── silver_salesorder.sql
│   ├── gold/                # Gold analytics tables
│   │   ├── gold_models.yml
│   │   ├── gold_sales_summary.sql
│   │   ├── gold_customer_lifetime_value.sql
│   │   └── gold_product_performance.sql
│   ├── staging/             # Source YAML (legacy location)
│   │   └── bronze.yml
│   └── marts/               # Dimensional models (SCD2-based)
│       ├── customer/
│       ├── product/
│       └── sales/
├── snapshots/               # SCD Type 2 snapshots
│   ├── customer.sql
│   ├── address.sql
│   ├── customeraddress.sql
│   ├── product.sql
│   ├── productmodel.sql
│   ├── salesorderheader.sql
│   └── salesorderdetail.sql
├── macros/
│   ├── generate_surrogate_key.sql
│   ├── cents_to_dollars.sql
│   └── dbt_current_timestamp.sql
└── tests/                   # Custom data tests
```

---

## Common dbt Commands

### Run all models

```bash
dbt run
```

### Run a specific layer

```bash
dbt run --select tag:bronze
dbt run --select tag:silver
dbt run --select tag:gold
```

### Run snapshots

```bash
dbt snapshot
```

### Run tests

```bash
dbt test
dbt test --select tag:silver  # test only silver models
```

### Check source freshness

```bash
dbt source freshness
```

### Generate and serve documentation

```bash
dbt docs generate
dbt docs serve
```

### Full refresh (rebuild all tables)

```bash
dbt run --full-refresh
```

---

## Macro Reference

### `generate_surrogate_key`

Generates an MD5 hash surrogate key from one or more columns.

```sql
select
    {{ generate_surrogate_key(['customer_id', 'address_id']) }} as sk,
    ...
```

### `cents_to_dollars`

Converts an integer cents column to a decimal dollar value.

```sql
select
    {{ cents_to_dollars('amount_cents') }} as amount_usd,
    ...
```

### `dbt_current_timestamp`

Returns the current UTC timestamp (adapter-agnostic wrapper).

```sql
select
    {{ dbt_current_timestamp() }} as loaded_at,
    ...
```

---

## Data Quality Tests

Tests are defined in YAML files alongside the models. Each layer has its own set:

### Source tests (`bronze_sources.yml`)
- `unique` and `not_null` on primary keys
- `freshness` checks on `ModifiedDate`

### Silver tests (`silver_models.yml`)
- `unique` and `not_null` on natural keys
- `not_null` on mandatory business columns

### Gold tests (`gold_models.yml`)
- `unique` and `not_null` on surrogate/natural keys
- `accepted_values` on categorical columns (e.g. `customer_segment`)

---

## SCD Type 2 Snapshots

Snapshots are run **after** bronze ingestion and **before** silver/gold transformation.

Each snapshot uses the `check` strategy on `all` columns, meaning any change to any column triggers a new version. The resulting columns are:

| Column          | Description                                      |
|-----------------|--------------------------------------------------|
| `dbt_scd_id`    | Unique ID for this version of the record         |
| `dbt_updated_at`| When this version was created                    |
| `dbt_valid_from`| When this version became effective               |
| `dbt_valid_to`  | When this version was superseded (NULL = current)|

**Query current records:**
```sql
select * from {{ ref('customer_snapshot') }} where dbt_valid_to is null
```

**Query historical records:**
```sql
select * from {{ ref('customer_snapshot') }} where dbt_valid_to is not null
```

---

## Adding a New Model

1. Create a new `.sql` file in the appropriate layer folder.
2. Add a `{{ config(...) }}` block with the correct materialization and location.
3. Add the model definition to the corresponding `_models.yml` file with column descriptions and tests.
4. Run `dbt run --select <model_name>` to build it.
5. Run `dbt test --select <model_name>` to validate it.

---

## Troubleshooting

| Issue                              | Solution                                                    |
|------------------------------------|-------------------------------------------------------------|
| `Source not found`                 | Check `bronze_sources.yml` — schema and table names        |
| `Relation not found`               | Ensure the referenced model has been run first              |
| Snapshot creates duplicate rows    | Verify `unique_key` matches the source primary key          |
| Slow gold model builds             | Add `ZORDER BY` on join keys via `03_delta_lake_setup.py`   |
| `dbt test` failures on freshness   | Check ADF pipeline ran successfully and data was loaded     |
