# Assignment 4 DE2
################################################################
# Importing libraries
import datetime
import json
import os
from pathlib import Path

import boto3
import requests

#################### SETTING THE ENVIRONMENT #####################

# Create S3 bucket
S3_WIKI_BUCKET = ""
S3 = boto3.client("s3")
S3_WIKI_BUCKET = "ceu-alain-wikidata"

bucket_names = [bucket["Name"] for bucket in S3.list_buckets()["Buckets"]]

# Only create the bucket if it doesn't exist
if S3_WIKI_BUCKET not in bucket_names:
    S3.create_bucket(
        Bucket = S3_WIKI_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

# Checking solution
assert S3_WIKI_BUCKET != "", "Please set the S3_WIKI_BUCKET variable"
assert S3.list_objects(
    Bucket = S3_WIKI_BUCKET
), "The bucket {S3_WIKI_BUCKET} does not exist"

# Set the directories on local disk
AM_path = "C:/AM/AmCEU/classes/Data Engineering 2/Assignment 4/"
project_raw_dir = "raw-views"
project_json_dir = "data/views"

# Create the new "raw" directory, ignore if it already exists
RAW_dir = Path(AM_path + project_raw_dir)
RAW_dir.mkdir(exist_ok = True, parents = True)
print(f"Created directory {RAW_dir}")

# Create the new "json" directory, ignore if it already exists
JSON_dir = Path(AM_path + project_json_dir)
JSON_dir.mkdir(exist_ok = True, parents = True)
print(f"Created directory {JSON_dir}")

########################### GETTING THE DATA ###########################

# Setting the target date
DATE_PARAM = "2023-10-21"
date = datetime.datetime.strptime(DATE_PARAM, "%Y-%m-%d")

# Wikimedia API URL for the top 1000 articles of the day
url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia.org/all-access/{date.strftime('%Y/%m/%d')}"
print(f"Requesting REST API URL: {url}")

# Getting response from Wikimedia API
wiki_server_response = requests.get(url, headers = {"User-Agent": "curl/7.68.0"})
wiki_response_status = wiki_server_response.status_code
wiki_response_body = wiki_server_response.text

print(f"Wikipedia REST API Response body: {wiki_response_body}")
print(f"Wikipedia REST API Response Code: {wiki_response_status}")

# Check if response status is not OK
if wiki_response_status != 200:
    print(
        f"Received non-OK status code from Wiki Server: {wiki_response_status}. Response body: {wiki_response_body}"
    )

########################### SAVING THE DATA ###########################

### Save Raw Response locally

# The file is named in the format `raw-views-YYYY-MM-DD.txt`
raw_views_file = RAW_dir / f"raw-views-{DATE_PARAM}.txt"
with raw_views_file.open("w", encoding="utf-8") as file:
    file.write(wiki_response_body)
    print(f"Saved raw views to {raw_views_file}")

# Upload the file to S3 and check presence
res = S3.upload_file(raw_views_file, S3_WIKI_BUCKET, f"datalake/raw/raw-views-{DATE_PARAM}.txt")
print(f"Uploaded raw views to S3://{S3_WIKI_BUCKET}/datalake/raw/raw-views-{DATE_PARAM}.txt")
assert S3.head_object(Bucket = S3_WIKI_BUCKET, Key = f"datalake/raw/raw-views-{DATE_PARAM}.txt")

### Parse the Wikipedia response and process the data as JSON
wiki_response_parsed = wiki_server_response.json()
wiki_edits = wiki_response_parsed["items"][0]["articles"]

# Convert server's response to file JSON_lines
current_time = datetime.datetime.utcnow()
json_lines = ""
for page in wiki_edits:
    record = {
        "article": page["article"],
        "views": page["views"],
        "rank": page["rank"],
        "date": DATE_PARAM,
        "retrieved_at": current_time.isoformat(),
    }
    json_lines += json.dumps(record) + "\n"

### Save JSON file to local directory
json_wiki_filename = f"views-{DATE_PARAM}.json"
json_wiki_file = JSON_dir / json_wiki_filename
with json_wiki_file.open("w") as file:
    file.write(json_lines)
    print(f"Saved JSON file to {json_wiki_file}")

raw_views_file = RAW_dir / f"raw-views-{DATE_PARAM}.txt"
with raw_views_file.open("w", encoding="utf-8") as file:
    file.write(wiki_response_body)
    print(f"Saved raw views to {raw_views_file}")

# Upload the JSON file to S3/datalake/views
SUB_DIR = "/datalake/views"
JSON_S3_DIR = Path(S3_WIKI_BUCKET + SUB_DIR)
 
res = S3.upload_file(json_wiki_file, S3_WIKI_BUCKET, f"datalake/views/views-{DATE_PARAM}.json")
print(f"Uploaded json views to S3://{S3_WIKI_BUCKET}/datalake/views/views-{DATE_PARAM}.json")
assert S3.head_object(Bucket = S3_WIKI_BUCKET, Key = f"datalake/views/views-{DATE_PARAM}.json")

