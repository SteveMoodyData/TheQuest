#!/usr/bin/env python3
"""
Downloads BLS time series data to S3
"""

import requests
import boto3
import re
from datetime import datetime

# Configuration
BASE_URL = 'https://download.bls.gov/pub/time.series/pr/'
S3_BUCKET = 'steve-m-bls-demo-bucket'
S3_PREFIX = 'bls/pr/'
HEADERS = {'User-Agent': 'Steve Moody steve.moody@gmail.com Data Quest'}

def get_file_list():
    """Scrape directory listing"""
    response = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    
    # Get all files from /pub/time.series/pr/
    files = re.findall(r'HREF="/pub/time\.series/pr/([^"]+)"', response.text)

    # Extract filenames only
    return [f.split('/')[-1] for f in files]

def sync_to_s3():
    s3 = boto3.client('s3')
    
    files = get_file_list()
    print(f"Found {len(files)} files:")
    
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
            print(f"{filename}: {e}")
            stats['errors'] += 1
    
    print(f"\n New: {stats['new']} | Skipped: {stats['skipped']} | Errors: {stats['errors']}")
    return stats

if __name__ == '__main__':
    sync_to_s3()