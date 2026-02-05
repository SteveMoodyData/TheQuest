# Databricks notebook source
# MAGIC %md
# MAGIC # Fetch Population API Data to Volume

# COMMAND ----------
import requests
import json
from datetime import datetime

# API URL
api_url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

# Volume path (Unity Catalog)
volume_path = "/Volumes/thequest/bronze/datausa_io"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fetch API Data

# COMMAND ----------
# Fetch data
response = requests.get(api_url)
response.raise_for_status()

data = response.json()

print(f"Fetched {len(data['data'])} records")
print(f"Response keys: {list(data.keys())}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save to Volume

# COMMAND ----------
# Add metadata
output_data = {
    "metadata": {
        "source_url": api_url,
        "fetched_at": datetime.utcnow().isoformat(),
        "record_count": len(data.get('data', []))
    },
    "data": data
}

# Create filename with timestamp
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
file_path = f"{volume_path}/population_data_{timestamp}.json"

# Write JSON to volume
with open(file_path, 'w') as f:
    json.dump(output_data, f, indent=2)

print(f"Saved to: {file_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------
# List files in volume
files = dbutils.fs.ls(volume_path)
print(f"Files in {volume_path}:")
for f in files:
    print(f"  {f.name} - {f.size:,} bytes")

# COMMAND ----------
# Read back and show sample
with open(file_path, 'r') as f:
    saved_data = json.load(f)

print(f"\nMetadata:")
print(json.dumps(saved_data['metadata'], indent=2))

print(f"\nFirst 3 data records:")
print(json.dumps(saved_data['data']['data'][:3], indent=2))
