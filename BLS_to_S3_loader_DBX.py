# Databricks notebook source
# MAGIC %md
# MAGIC # BLS Data Sync to S3 (with Databricks Secrets)

# COMMAND ----------
import requests
import boto3
import re
from datetime import datetime

# Configuration
BASE_URL = 'https://download.bls.gov/pub/time.series/pr/'
S3_BUCKET = 'steve-m-bls-demo-bucket'
S3_PREFIX = 'bls/pr/'
HEADERS = {'User-Agent': 'Steve Moody steve.moody@gmail.com Data Quest'}

# Get AWS credentials from Databricks secrets
# Replace 'aws' with your actual secret scope name
AWS_ACCESS_KEY = dbutils.secrets.get(scope="aws", key="access_key_id")
AWS_SECRET_KEY = dbutils.secrets.get(scope="aws", key="secret_access_key")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Get File List from BLS

# COMMAND ----------
def get_file_list():
    """Scrape directory listing"""
    response = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    
    # Get all files from /pub/time.series/pr/
    files = re.findall(r'HREF="/pub/time\.series/pr/([^"]+)"', response.text)
    
    return files

# Test it
files = get_file_list()
print(f"Found {len(files)} files:")
for f in files[:5]:
    print(f"  {f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sync to S3

# COMMAND ----------
def sync_to_s3():
    """Download BLS files and upload to S3"""
    
    # Create S3 client with credentials from secrets
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    
    files = get_file_list()
    print(f"Found {len(files)} files\n")
    
    stats = {'new': 0, 'skipped': 0, 'errors': 0}
    
    for filename in files:
        s3_key = f"{S3_PREFIX}{filename}"
        
        # Skip if exists
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
            print(f" {filename}")
            stats['skipped'] += 1
            continue
        except:
            pass
        
        # Download and upload
        try:
            url = f"{BASE_URL}{filename}"
            data = requests.get(url, headers=HEADERS, timeout=30)
            data.raise_for_status()
            
            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=data.content)
            print(f"{filename} ({len(data.content):,} bytes)")
            stats['new'] += 1
            
        except Exception as e:
            print(f" {filename}: {e}")
            stats['errors'] += 1
    
    print("\n" + "="*60)
    print(f"📊 New: {stats['new']} | Skipped: {stats['skipped']} | Errors: {stats['errors']}")
    print("="*60)
    
    return stats

# Run the sync
results = sync_to_s3()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify Files in S3

# COMMAND ----------
# List files in S3 to verify
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

if 'Contents' in response:
    print(f"Files in s3://{S3_BUCKET}/{S3_PREFIX}:\n")
    for obj in response['Contents']:
        print(f"  {obj['Key']} - {obj['Size']:,} bytes")
else:
    print("No files found")