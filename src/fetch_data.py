import boto3


def fetch_data(**kwargs):
    # Connect to the S3 bucket
    s3 = boto3.client("s3")

    # Set the name of the bucket and the key prefix
    bucket = "de-tech-assessment-2022"
    prefix = "data/"

    # List all the objects in the bucket
    objects = s3.list_objects(Bucket=bucket, Prefix=prefix)

    # Iterate over the objects and download them
    for obj in objects["Contents"]:
        key = obj["Key"]
        filename = key.replace(prefix, "")
        s3.download_file(bucket, key, filename)
        print(f"Downloaded {key} to {filename}")
