import boto3
import os

from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

def fetch_data(**kwargs):
    # Connect to the S3 bucket
    s3 = boto3.client("s3")

    # Set the name of the bucket and the key prefix
    bucket = os.environ["S3_BUCKET"]

     # Get the current execution date from the kwargs dictionary
    execution_date = kwargs["execution_date"]

    # Calculate the date for the previous day
    yesterday = execution_date - timedelta(days=1)

    # Format the date as a string in the format yyyy-mm-dd
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # List the objects in the S3 bucket that have the date in the file name
    objects = s3.list_objects(Bucket="de-tech-assessment-2022", Prefix=f"data/{yesterday_str}")

    # Iterate over the objects and download them
    for obj in objects["Contents"]:
        key = obj["Key"]
        filename = key.replace(prefix, "")
        s3.download_file(bucket, key, "data/" + filename)
        print(f"Downloaded {key} to {filename}")
