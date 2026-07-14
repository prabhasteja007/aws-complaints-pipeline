import boto3
import os
from tqdm import tqdm

BUCKET = "complaints-pipeline-teja"
LOCAL_FILE = "data/complaints.csv"
S3_KEY = "raw/complaints.csv"

s3 = boto3.client("s3", region_name="us-east-1")

file_size = os.path.getsize(LOCAL_FILE)
print(f"Uploading {LOCAL_FILE} ({file_size / (1024*1024):.1f} MB) to s3://{BUCKET}/{S3_KEY}")

with tqdm(total=file_size, unit="B", unit_scale=True, desc="Uploading") as bar:
    s3.upload_file(
        LOCAL_FILE,
        BUCKET,
        S3_KEY,
        Callback=lambda bytes_transferred: bar.update(bytes_transferred),
    )

print(f"\nDone. Verifying...")
response = s3.head_object(Bucket=BUCKET, Key=S3_KEY)
size_mb = response["ContentLength"] / (1024 * 1024)
print(f"File in S3: s3://{BUCKET}/{S3_KEY} ({size_mb:.1f} MB)")
print("Upload successful!")
