# Databricks notebook source
# title: 02_silver_transformation
# description: Apply data quality rules and transformations to produce Silver Delta tables

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Layer — Data Cleaning & Standardisation
# MAGIC
# MAGIC This notebook reads Bronze Delta tables and produces cleaned, standardised Silver
# MAGIC tables with:
# MAGIC - Deduplication using `row_number()` window functions
# MAGIC - Null handling and empty-string normalisation
# MAGIC - Correct data type casting
# MAGIC - Derived columns (e.g. `FullName`)
# MAGIC
# MAGIC **Note:** The canonical transformation logic lives in dbt silver models. This notebook
# MAGIC is provided as a reference/backfill utility and for teams that prefer pure PySpark.

# COMMAND ----------
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
SILVER_BASE   = "/mnt/silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# COMMAND ----------
# ── Customers ────────────────────────────────────────────────────────────────

window_cust = Window.partitionBy("CustomerID").orderBy(F.desc("ModifiedDate"))

silver_customer = (
    spark.table(f"{BRONZE_SCHEMA}.customer")
         .withColumn("_rn", F.row_number().over(window_cust))
         .filter(F.col("_rn") == 1)
         .filter(F.col("CustomerID").isNotNull())
         .select(
             "CustomerID",
             F.trim("Title").alias("Title"),
             F.trim("FirstName").alias("FirstName"),
             F.trim("MiddleName").alias("MiddleName"),
             F.trim("LastName").alias("LastName"),
             F.lower(F.trim("EmailAddress")).alias("EmailAddress"),
             F.trim("Phone").alias("Phone"),
             F.trim("CompanyName").alias("CompanyName"),
             F.concat_ws(
                 " ",
                 F.nullif(F.trim("FirstName"), F.lit("")),
                 F.nullif(F.trim("MiddleName"), F.lit("")),
                 F.nullif(F.trim("LastName"), F.lit("")),
             ).alias("FullName"),
             F.col("ModifiedDate").cast("timestamp"),
             F.current_timestamp().alias("_loaded_at"),
         )
)

(
    silver_customer.write
                   .format("delta")
                   .mode("overwrite")
                   .option("overwriteSchema", "true")
                   .option("path", f"{SILVER_BASE}/customer")
                   .saveAsTable(f"{SILVER_SCHEMA}.customer")
)
logger.info("silver.customer written")

# COMMAND ----------
# ── Products ──────────────────────────────────────────────────────────────────

window_prod = Window.partitionBy("ProductID").orderBy(F.desc("ModifiedDate"))

silver_product = (
    spark.table(f"{BRONZE_SCHEMA}.product")
         .withColumn("_rn", F.row_number().over(window_prod))
         .filter(F.col("_rn") == 1)
         .filter(F.col("ProductID").isNotNull())
         .select(
             "ProductID",
             F.trim("Name").alias("Name"),
             F.trim("ProductNumber").alias("ProductNumber"),
             F.nullif(F.trim("Color"), F.lit("")).alias("Color"),
             F.col("StandardCost").cast("decimal(19,4)"),
             F.col("ListPrice").cast("decimal(19,4)"),
             F.nullif(F.trim("Size"), F.lit("")).alias("Size"),
             F.col("Weight").cast("decimal(8,2)"),
             "ProductCategoryID",
             "ProductModelID",
             F.col("SellStartDate").cast("date"),
             F.col("SellEndDate").cast("date"),
             F.col("DiscontinuedDate").cast("date"),
             F.col("ModifiedDate").cast("timestamp"),
             F.current_timestamp().alias("_loaded_at"),
         )
)

(
    silver_product.write
                  .format("delta")
                  .mode("overwrite")
                  .option("overwriteSchema", "true")
                  .option("path", f"{SILVER_BASE}/product")
                  .saveAsTable(f"{SILVER_SCHEMA}.product")
)
logger.info("silver.product written")

# COMMAND ----------
# ── Sales Orders ─────────────────────────────────────────────────────────────

window_hdr = Window.partitionBy("SalesOrderID").orderBy(F.desc("ModifiedDate"))
window_dtl = Window.partitionBy("SalesOrderDetailID").orderBy(F.desc("ModifiedDate"))

header_clean = (
    spark.table(f"{BRONZE_SCHEMA}.salesorderheader")
         .withColumn("_rn", F.row_number().over(window_hdr))
         .filter(F.col("_rn") == 1)
         .filter(F.col("SalesOrderID").isNotNull())
)

detail_clean = (
    spark.table(f"{BRONZE_SCHEMA}.salesorderdetail")
         .withColumn("_rn", F.row_number().over(window_dtl))
         .filter(F.col("_rn") == 1)
         .filter(F.col("SalesOrderDetailID").isNotNull())
)

silver_salesorder = (
    detail_clean.alias("d")
                .join(header_clean.alias("h"), "SalesOrderID", "inner")
                .select(
                    F.col("d.SalesOrderDetailID"),
                    F.col("d.SalesOrderID"),
                    F.col("h.SalesOrderNumber"),
                    F.col("h.CustomerID"),
                    F.col("h.OrderDate").cast("date"),
                    F.col("h.DueDate").cast("date"),
                    F.col("h.ShipDate").cast("date"),
                    F.col("h.Status").cast("tinyint"),
                    F.col("h.OnlineOrderFlag").cast("boolean"),
                    F.col("h.SubTotal").cast("decimal(19,4)"),
                    F.col("h.TaxAmt").cast("decimal(19,4)"),
                    F.col("h.Freight").cast("decimal(19,4)"),
                    F.col("h.TotalDue").cast("decimal(19,4)"),
                    F.col("d.ProductID"),
                    F.col("d.OrderQty").cast("int"),
                    F.col("d.UnitPrice").cast("decimal(19,4)"),
                    F.col("d.UnitPriceDiscount").cast("decimal(5,4)"),
                    F.col("d.LineTotal").cast("decimal(38,6)"),
                    F.col("h.ModifiedDate").cast("timestamp"),
                    F.current_timestamp().alias("_loaded_at"),
                )
)

(
    silver_salesorder.write
                     .format("delta")
                     .mode("overwrite")
                     .option("overwriteSchema", "true")
                     .option("path", f"{SILVER_BASE}/salesorder")
                     .saveAsTable(f"{SILVER_SCHEMA}.salesorder")
)
logger.info("silver.salesorder written")

# COMMAND ----------
# MAGIC %md ## Row count validation

# COMMAND ----------
for tbl in ["customer", "product", "salesorder"]:
    count = spark.table(f"{SILVER_SCHEMA}.{tbl}").count()
    print(f"{SILVER_SCHEMA}.{tbl:<15}: {count:>10,} rows")
