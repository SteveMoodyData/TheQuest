# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1: BLS Data Scraper to S3
# MAGIC Scrapes BLS website and writes files to steve-m-bls-demo-bucket

# COMMAND ----------
# MAGIC %pip install beautifulsoup4 requests lxml

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib

# Parameters
dbutils.widgets.text("landing_bucket", "s3://steve-m-bls-demo-bucket")
landing_bucket = dbutils.widgets.get("landing_bucket")

# Configuration
BASE_URL = 'https://download.bls.gov/pub/time.series/pr/'
HEADERS = {
    'User-Agent': 'Steve Moody steve.moody@gmail.com Databricks Rearc Quest Project'
}
LANDING_PATH = f"{landing_bucket}/bls/pr/"

print(f"Landing path: {LANDING_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Scrape Directory Listing

# COMMAND ----------
def get_file_list(url):
    """Scrape directory listing from BLS website"""
    print(f"Fetching file list from: {url}")
    
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all links ending in .txt
    files = []
    for link in soup.find_all('a'):
        href = link.get('href', '')
        if href.endswith('.txt'):
            files.append(href)
    
    return files

try:
    files_to_download = get_file_list(BASE_URL)
    print(f"\n Found {len(files_to_download)} files to process\n")
    print("First 10 files:")
    for f in files_to_download[:10]:
        print(f"  - {f}")
except Exception as e:
    print(f" Error fetching file list: {str(e)}")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## Download Files to S3 (Idempotent)

# COMMAND ----------
def file_exists_in_s3(path):
    """Check if file exists in S3"""
    try:
        dbutils.fs.head(path, 1)
        return True
    except:
        return False

def download_file_to_s3(file_name, force=False):
    """Download file from BLS and write to S3 if changed or new"""
    source_url = f"{BASE_URL}{file_name}"
    target_path = f"{LANDING_PATH}{file_name}"
    
    # Check if exists (unless force)
    if not force and file_exists_in_s3(target_path):
        # For demo purposes, you could add checksum comparison here
        # For now, we'll skip existing files
        print(f"⏭️  SKIP: {file_name} (already exists)")
        return 'skipped'
    
    try:
        # Download file with timeout
        response = requests.get(source_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        # Write to temp location first
        temp_path = f"/tmp/{file_name}"
        with open(temp_path, 'wb') as f:
            f.write(response.content)
        
        # Copy to S3
        dbutils.fs.cp(f"file:{temp_path}", target_path, recurse=False)
        
        # Clean up temp
        import os
        os.remove(temp_path)
        
        print(f" DOWNLOADED: {file_name} ({len(response.content):,} bytes)")
        return 'downloaded'
        
    except Exception as e:
        print(f" ERROR downloading {file_name}: {str(e)}")
        return 'error'

# COMMAND ----------
# MAGIC %md
# MAGIC ## Download All Files

# COMMAND ----------
# Download all files
results = {'downloaded': 0, 'skipped': 0, 'errors': 0}

for file_name in files_to_download:
    result = download_file_to_s3(file_name, force=False)
    results[result] += 1

# COMMAND ----------
# Return for job tracking
dbutils.notebook.exit(str(results))
