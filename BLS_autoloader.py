# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader: Each BLS File to Separate Table

# COMMAND ----------

from pyspark.sql.functions import *

# Configuration
source_path = "s3://steve-m-bls-demo-bucket/bls/pr/"
checkpoint_path = "s3://steve-m-bls-demo-bucket/checkpoints/bls_current/"
target_table = "thequest.bronze.bls_pr_data_current"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader for pr.data.0.Current

# COMMAND ----------

# Auto Loader with file pattern filter
df_raw = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", f"{checkpoint_path}_schema")
  .option("header", "true")
  .option("sep", "\t")
  .option("inferSchema", "true")
  .option("pathGlobFilter", "pr.data.0.Current")
  .load(source_path)
  .withColumn("_source_file", col("_metadata.file_path"))  # Unity Catalog way
  .withColumn("_ingestion_time", current_timestamp())
)




# Clean column names - remove trailing/leading spaces
df_clean = df_raw.select([
    col(c).alias(c.strip()) for c in df_raw.columns
])

# Add metadata columns
df = (df_clean
  .withColumn("_source_file", col("_metadata.file_path"))
  .withColumn("_ingestion_time", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table

# COMMAND ----------

# Write stream
query = (df.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_path)
  .option("mergeSchema", "true")
  .trigger(availableNow=True)
  .toTable(target_table))

query.awaitTermination()


print(f"Data loaded to {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------


# Row count
row_count = spark.table(target_table).count()
print(f"Total rows: {row_count:,}")

# COMMAND ----------

# Check the data
display(spark.table(target_table).limit(100))

