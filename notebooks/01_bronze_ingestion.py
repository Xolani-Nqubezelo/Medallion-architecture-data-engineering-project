# Databricks notebook source
# title: 01_bronze_ingestion
# description: Ingest raw data from Azure SQL Database into Bronze Delta tables using ADF triggers

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC This notebook reads data from the Azure SQL Database (AdventureWorksLT) that has been
# MAGIC landed in ADLS Gen2 by Azure Data Factory and registers each table as a Delta Lake table
# MAGIC in the Hive metastore under the `bronze` schema.
# MAGIC
# MAGIC **Data Flow:**
# MAGIC ```
# MAGIC Azure SQL DB ──ADF──> ADLS Gen2 (Parquet) ──Spark──> Delta (bronze schema)
# MAGIC ```

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# Configuration
ADLS_ACCOUNT   = spark.conf.get("adls.account.name", "youradlsaccount")
CONTAINER      = spark.conf.get("adls.container", "raw")
BASE_PATH      = f"abfss://{CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"
BRONZE_SCHEMA  = "bronze"
BRONZE_BASE    = "/mnt/bronze"

# AdventureWorksLT tables to ingest
TABLES = [
    "address",
    "customer",
    "customeraddress",
    "product",
    "productcategory",
    "productdescription",
    "productmodel",
    "salesorderdetail",
    "salesorderheader",
]

# COMMAND ----------
# Ensure the bronze schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")

# COMMAND ----------
def ingest_table(table_name: str) -> None:
    """
    Read the latest Parquet snapshot from ADLS and write it as a Delta table
    in the bronze schema. Uses MERGE to upsert on ModifiedDate, preventing
    duplicate loads during re-runs.
    """
    source_path = f"{BASE_PATH}/adventureworks/{table_name}/"
    target_path = f"{BRONZE_BASE}/{table_name}"
    full_table   = f"{BRONZE_SCHEMA}.{table_name}"

    logger.info(f"Ingesting {table_name} from {source_path}")

    df = (
        spark.read
             .format("parquet")
             .option("inferSchema", "true")
             .load(source_path)
             .withColumn("_source_path", lit(source_path))
             .withColumn("_loaded_at", current_timestamp())
    )

    if not spark.catalog.tableExists(full_table):
        # Initial load — create table
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .option("path", target_path)
              .saveAsTable(full_table)
        )
        logger.info(f"Created table {full_table}")
    else:
        # Incremental load — merge on natural key
        deltaTable = DeltaTable.forName(spark, full_table)
        (
            deltaTable.alias("target")
                      .merge(df.alias("source"), "target.rowguid = source.rowguid")
                      .whenMatchedUpdateAll()
                      .whenNotMatchedInsertAll()
                      .execute()
        )
        logger.info(f"Merged into {full_table}")

    # Collect basic stats for audit logging
    count = spark.table(full_table).count()
    logger.info(f"{full_table}: {count:,} rows after ingest")

# COMMAND ----------
for table in TABLES:
    try:
        ingest_table(table)
    except Exception as e:
        logger.error(f"Failed to ingest {table}: {e}")
        raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validation — Row Counts per Table
# MAGIC Quick sanity check after ingestion.

# COMMAND ----------
for table in TABLES:
    count = spark.table(f"{BRONZE_SCHEMA}.{table}").count()
    print(f"{BRONZE_SCHEMA}.{table:<25}: {count:>10,} rows")
