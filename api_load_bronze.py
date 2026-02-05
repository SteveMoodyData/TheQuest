# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch Population API Data

# COMMAND ----------
import requests

# API URL
api_url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

# Fetch data
response = requests.get(api_url)
data = response.json()

print(f"Fetched {len(data['data'])} records")

# COMMAND ----------
# Convert to Spark DataFrame
df = spark.createDataFrame(data['data'])

display(df)

# COMMAND ----------
# Save as Delta table
df.write.mode("overwrite").saveAsTable("thequest.bronze.honolulu_population_data")

print("Saved to thequest.bronze.honolulu_population_data")
