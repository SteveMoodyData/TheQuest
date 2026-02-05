# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader: Each BLS File to Separate Table

# COMMAND ----------
from pyspark.sql.functions import *

# Configuration
source_path = "s3://steve-m-bls-demo-bucket/bls/pr/"
checkpoint_base = "s3://steve-m-bls-demo-bucket/checkpoints/bls/"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Get List of Files

# COMMAND ----------
files = dbutils.fs.ls(source_path)

print(f"Found {len(files)} files:\n")
for f in files:
    print(f"  {f.name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Each File to Separate Table

# COMMAND ----------
results = []

for file_info in files:
    file_name = file_info.name
    
    # Create table name from filename (replace dots with underscores)
    table_name = f"default.bls_{file_name.replace('.', '_')}"
    
    # Paths for this specific file
    file_path = f"{source_path}{file_name}"
    checkpoint_path = f"{checkpoint_base}{file_name}/"
    
    print(f"\n{'='*60}")
    print(f"Processing: {file_name}")
    print(f"Target table: {table_name}")
    print(f"{'='*60}")
    
    try:
        # Auto Loader for this specific file
        df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", f"{checkpoint_path}_schema")
          .option("header", "true")
          .option("sep", "\t")
          .option("inferSchema", "true")
          .load(file_path)
          .withColumn("_source_file", lit(file_name))
          .withColumn("_ingestion_time", current_timestamp())
        )
        
        # Write to table
        query = (df.writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", checkpoint_path)
          .option("mergeSchema", "true")
          .trigger(availableNow=True)
          .toTable(table_name))
        
        query.awaitTermination()
        
        # Get row count
        row_count = spark.table(table_name).count()
        print(f"SUCCESS: {row_count:,} rows")
        
        results.append({
            'file': file_name,
            'table': table_name,
            'rows': row_count,
            'status': 'success'
        })
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        results.append({
            'file': file_name,
            'table': table_name,
            'rows': 0,
            'status': f'error: {str(e)}'
        })

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
# Display results
results_df = spark.createDataFrame(results)
display(results_df)

# COMMAND ----------
# Show all tables created
print("\nTables created:")
for r in results:
    if r['status'] == 'success':
        print(f"  {r['table']} ({r['rows']:,} rows)")
