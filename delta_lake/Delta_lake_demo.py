# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake: ACID Properties & Time Travel Demo
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC - Creating Delta tables with ACID guarantees
# MAGIC - Concurrent writes with isolation
# MAGIC - Time Travel for auditing and rollback
# MAGIC - Schema enforcement
# MAGIC - Performance optimization tips

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Initialize Spark with Delta Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp
import time

# Enable Delta optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Set up storage path (adjust for your environment)
delta_path = "/Volumes/workspace/siddatasets/filedatasets/landing/deltademo/"

# Clean up if exists from previous runs
dbutils.fs.rm(delta_path, recurse=True)

print("✅ Configuration complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Create Your First Delta Table

# COMMAND ----------

# Sample customer data
data = [
    (1, "Acme Corp", 50000, "2026-01-15"),
    (2, "TechStart Inc", 75000, "2026-01-20"),
    (3, "Global Industries", 120000, "2026-01-25"),
    (4, "DataFlow Systems", 95000, "2026-01-28")
]

df = spark.createDataFrame(data, ["customer_id", "company_name", "revenue", "transaction_date"])

# Write as Delta table
df.write.format("delta").mode("overwrite").save(delta_path)

print("✅ Delta table created")
display(spark.read.format("delta").load(delta_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: ACID - Isolation (Concurrent Writes)

# COMMAND ----------

# Load Delta table
delta_table = DeltaTable.forPath(spark, delta_path)

# Simulate two jobs updating the same customer simultaneously
print("📝 Job 1: Updating revenue for customer_id = 1")
delta_table.update(
    condition = "customer_id = 1",
    set = {"revenue": "55000"}
)

print("📝 Job 2: Updating company name for customer_id = 1")
delta_table.update(
    condition = "customer_id = 1",
    set = {"company_name": "Acme Corporation"}
)

print("\n✅ Both updates succeeded without conflicts!")
print("Current state of customer_id = 1:")
display(spark.read.format("delta").load(delta_path).filter("customer_id = 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: ACID - Atomicity (All or Nothing)

# COMMAND ----------

# Attempt a batch update - if ANY row fails, ALL rows rollback
try:
    delta_table.update(
        condition = "revenue > 50000",
        set = {"revenue": "revenue * 1.1"}  # 10% increase
    )
    print("✅ Batch update completed atomically - all qualifying rows updated")
    display(spark.read.format("delta").load(delta_path))
except Exception as e:
    print(f"❌ Update failed and rolled back: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Time Travel - Version History

# COMMAND ----------

# View complete transaction history
print("📜 Transaction History:")
display(delta_table.history())

# Show key columns
print("\n📊 Summary of Changes:")
display(delta_table.history().select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Time Travel - Read Previous Versions

# COMMAND ----------

# Read version 0 (original data)
print("📂 Version 0 (Original):")
v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
display(v0.filter("customer_id = 1"))

# Read version 1 (after first update)
print("\n📂 Version 1 (After revenue update):")
v1 = spark.read.format("delta").option("versionAsOf", 1).load(delta_path)
display(v1.filter("customer_id = 1"))

# Read current version
print("\n📂 Current Version:")
current = spark.read.format("delta").load(delta_path)
display(current.filter("customer_id = 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Time Travel by Timestamp

# COMMAND ----------

from datetime import datetime, timedelta

# Get timestamp from 5 minutes ago (adjust as needed)
five_min_ago = (datetime.now() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")

print(f"📅 Reading data as of: {five_min_ago}")

try:
    historical_df = spark.read.format("delta") \
        .option("timestampAsOf", five_min_ago) \
        .load(delta_path)
    display(historical_df)
except Exception as e:
    print(f"⚠️ Timestamp too old or before table creation: {e}")
    print("Showing version 0 instead:")
    display(spark.read.format("delta").option("versionAsOf", 0).load(delta_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Schema Enforcement (Consistency)

# COMMAND ----------

# Try to insert data with WRONG schema
print("🧪 Attempting to insert bad data (string in revenue column)...")

bad_data = [(5, "Invalid Corp", "NOT_A_NUMBER", "2026-02-01")]
bad_df = spark.createDataFrame(bad_data, ["customer_id", "company_name", "revenue", "transaction_date"])

try:
    bad_df.write.format("delta").mode("append").save(delta_path)
    print("❌ This shouldn't print - bad data was accepted!")
except Exception as e:
    print(f"✅ Delta correctly rejected bad data!")
    print(f"Error: {str(e)[:200]}...")

# COMMAND ----------

# Try to insert data with MISSING column
print("\n🧪 Attempting to insert data with missing column...")

incomplete_data = [(6, "Incomplete Corp", 80000)]
incomplete_df = spark.createDataFrame(incomplete_data, ["customer_id", "company_name", "revenue"])

try:
    incomplete_df.write.format("delta").mode("append").save(delta_path)
    print("❌ This shouldn't print - incomplete data was accepted!")
except Exception as e:
    print(f"✅ Delta correctly rejected incomplete data!")
    print(f"Error: {str(e)[:200]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Schema Evolution (Controlled Changes)

# COMMAND ----------

# Add a new column using schema evolution
new_data = [
    (5, "Quantum Systems", 110000, "2026-02-01", "Enterprise")
]

new_df = spark.createDataFrame(
    new_data, 
    ["customer_id", "company_name", "revenue", "transaction_date", "customer_tier"]
)

print("📝 Adding data with NEW column (customer_tier)...")

# Enable schema evolution
new_df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(delta_path)

print("✅ Schema evolved successfully!")
display(spark.read.format("delta").load(delta_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 9: Merge (Upsert) Operations

# COMMAND ----------

# Prepare updates and new records
updates_data = [
    (1, "Acme Corporation Ltd", 60000, "2026-02-01", "Premium"),  # Update existing
    (6, "NewCo Industries", 85000, "2026-02-01", "Standard")      # Insert new
]

updates_df = spark.createDataFrame(
    updates_data,
    ["customer_id", "company_name", "revenue", "transaction_date", "customer_tier"]
)

print("🔄 Performing MERGE (upsert) operation...")

delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdate(
    set = {
        "company_name": "source.company_name",
        "revenue": "source.revenue",
        "transaction_date": "source.transaction_date",
        "customer_tier": "source.customer_tier"
    }
).whenNotMatchedInsert(
    values = {
        "customer_id": "source.customer_id",
        "company_name": "source.company_name",
        "revenue": "source.revenue",
        "transaction_date": "source.transaction_date",
        "customer_tier": "source.customer_tier"
    }
).execute()

print("✅ Merge complete!")
display(spark.read.format("delta").load(delta_path).orderBy("customer_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 10: Delete Operations

# COMMAND ----------

print("🗑️ Deleting customers with revenue < 80000...")

delta_table.delete("revenue < 80000")

print("✅ Delete complete!")
display(spark.read.format("delta").load(delta_path))

# Can still access deleted data via Time Travel!
print("\n🕐 But we can still see deleted data in previous versions:")
display(spark.read.format("delta").option("versionAsOf", 0).load(delta_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 11: OPTIMIZE - Compaction

# COMMAND ----------

print("🔧 Running OPTIMIZE to compact small files...")

# This command compacts small files into larger ones
spark.sql(f"OPTIMIZE delta.`{delta_path}`")

print("✅ Optimization complete!")

# Check file statistics
print("\nFile details:")
display(spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}`"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 12: Z-ORDER (Advanced Optimization)

# COMMAND ----------

print("🎯 Running Z-ORDER on revenue column for faster filtering...")

# Z-ORDER co-locates related data for better query performance
spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY (revenue)")

print("✅ Z-ORDER complete! Queries filtering by revenue will be faster.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 13: VACUUM - Cleanup Old Versions

# COMMAND ----------

print("🧹 Current transaction history:")
display(delta_table.history().select("version", "timestamp"))

print("\n⚠️ VACUUM will delete files not required by versions older than retention period")
print("Default retention: 7 days (168 hours)")

# For demo purposes, we'll use 0 hours (NOT recommended in production!)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

print("\n🗑️ Running VACUUM with 0 hour retention (demo only)...")
delta_table.vacuum(0)

print("✅ VACUUM complete!")

# Try to read old version (will fail after VACUUM)
try:
    spark.read.format("delta").option("versionAsOf", 0).load(delta_path).display()
except Exception as e:
    print(f"\n⚠️ Version 0 files deleted by VACUUM: {str(e)[:150]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Key Takeaways
# MAGIC
# MAGIC ✅ **ACID guarantees** make Delta production-ready  
# MAGIC ✅ **Time Travel** enables auditing and rollback  
# MAGIC ✅ **Schema enforcement** prevents data quality issues  
# MAGIC ✅ **Merge/Upsert** simplifies CDC patterns  
# MAGIC ✅ **OPTIMIZE & Z-ORDER** improve query performance  
# MAGIC ✅ **VACUUM** manages storage costs  
# MAGIC
# MAGIC **Production best practices:**
# MAGIC - Enable `optimizeWrite` and `autoCompact`
# MAGIC - Set VACUUM retention to 7-30 days
# MAGIC - Use Z-ORDER on commonly filtered columns
# MAGIC - Monitor table history and file sizes
# MAGIC - Use MERGE for CDC instead of delete+insert

# COMMAND ----------

# Cleanup (optional)
# dbutils.fs.rm(delta_path, recurse=True)
