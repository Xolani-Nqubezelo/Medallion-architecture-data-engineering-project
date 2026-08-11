# Databricks notebook source
# title: 03_delta_lake_setup
# description: Delta Lake configuration, optimization, and performance tuning

# COMMAND ----------
# MAGIC %md
# MAGIC # Delta Lake Setup & Performance Optimisation
# MAGIC
# MAGIC This notebook configures Delta Lake settings for all medallion layers:
# MAGIC - Auto-optimise and auto-compact
# MAGIC - Z-ORDER clustering on high-cardinality join keys
# MAGIC - Table statistics collection for the query optimiser
# MAGIC - Data retention and vacuum settings
# MAGIC - Time-travel configuration

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# ── Global Delta Lake settings ────────────────────────────────────────────────

spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------
# ── Schema / table map ────────────────────────────────────────────────────────

LAYER_CONFIG = {
    "bronze": {
        "tables": [
            "address", "customer", "customeraddress",
            "product", "productcategory", "productdescription",
            "productmodel", "salesorderdetail", "salesorderheader",
        ],
        "retention_hours": 720,   # 30 days
        "z_order": {
            "customer":          ["CustomerID"],
            "salesorderheader":  ["CustomerID", "OrderDate"],
            "salesorderdetail":  ["SalesOrderID", "ProductID"],
            "product":           ["ProductID", "ProductCategoryID"],
        },
    },
    "silver": {
        "tables": ["customer", "product", "salesorder"],
        "retention_hours": 720,
        "z_order": {
            "customer":   ["CustomerID"],
            "salesorder": ["CustomerID", "OrderDate", "ProductID"],
            "product":    ["ProductID"],
        },
    },
    "gold": {
        "tables": [
            "dim_customer", "dim_product",
            "gold_sales_summary", "gold_customer_lifetime_value",
            "gold_product_performance",
        ],
        "retention_hours": 2160,  # 90 days
        "z_order": {},
    },
}

# COMMAND ----------
def enable_change_data_feed(schema: str, table: str) -> None:
    """Enable Delta Change Data Feed on a table for incremental downstream reads."""
    try:
        spark.sql(f"""
            ALTER TABLE {schema}.{table}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        logger.info(f"CDF enabled on {schema}.{table}")
    except Exception as e:
        logger.warning(f"Could not enable CDF on {schema}.{table}: {e}")


def set_retention(schema: str, table: str, hours: int) -> None:
    spark.sql(f"""
        ALTER TABLE {schema}.{table}
        SET TBLPROPERTIES (
            delta.logRetentionDuration    = 'interval {hours} hours',
            delta.deletedFileRetentionDuration = 'interval {hours} hours'
        )
    """)
    logger.info(f"Retention set to {hours}h on {schema}.{table}")


def optimize_and_zorder(schema: str, table: str, cols: list) -> None:
    col_list = ", ".join(cols)
    spark.sql(f"OPTIMIZE {schema}.{table} ZORDER BY ({col_list})")
    logger.info(f"OPTIMIZE ZORDER({col_list}) on {schema}.{table}")


def vacuum_table(schema: str, table: str, retention_hours: int) -> None:
    spark.sql(f"VACUUM {schema}.{table} RETAIN {retention_hours} HOURS")
    logger.info(f"VACUUM {schema}.{table} completed")


def collect_stats(schema: str, table: str) -> None:
    spark.sql(f"ANALYZE TABLE {schema}.{table} COMPUTE STATISTICS FOR ALL COLUMNS")
    logger.info(f"Statistics collected for {schema}.{table}")

# COMMAND ----------
for layer, config in LAYER_CONFIG.items():
    logger.info(f"\n{'='*60}\nConfiguring {layer.upper()} layer\n{'='*60}")
    for tbl in config["tables"]:
        full = f"{layer}.{tbl}"
        if not spark.catalog.tableExists(full):
            logger.warning(f"Table {full} does not exist — skipping")
            continue

        set_retention(layer, tbl, config["retention_hours"])
        enable_change_data_feed(layer, tbl)

        if tbl in config["z_order"]:
            optimize_and_zorder(layer, tbl, config["z_order"][tbl])

        collect_stats(layer, tbl)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Delta Table Properties Report

# COMMAND ----------
for layer, config in LAYER_CONFIG.items():
    for tbl in config["tables"]:
        full = f"{layer}.{tbl}"
        if spark.catalog.tableExists(full):
            detail = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]
            print(f"{full:<45} | format={detail['format']:<6} | numFiles={detail['numFiles']:>5} | sizeInBytes={detail['sizeInBytes']:>15,}")
