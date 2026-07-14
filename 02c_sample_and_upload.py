import pandas as pd
import boto3
import os
from tqdm import tqdm

RAW_FILE = "data/complaints.csv"
SAMPLE_FILE = "data/complaints_500k.csv"
BUCKET = "complaints-pipeline-teja"
S3_KEY = "raw/complaints_500k.csv"

# ── Step 1: Read first 500K rows ───────────────────────────────────────────
print("Reading 500,000 rows from complaints.csv...")
df = pd.read_csv(RAW_FILE, nrows=500_000, low_memory=False)

print(f"Loaded: {len(df):,} rows")
print(f"Date range: {df['Date received'].min()} → {df['Date received'].max()}")
print(f"Columns: {list(df.columns)}")

df.to_csv(SAMPLE_FILE, index=False)
size_mb = os.path.getsize(SAMPLE_FILE) / (1024 * 1024)
print(f"Saved to {SAMPLE_FILE} ({size_mb:.1f} MB)")

# ── Step 2: Upload to S3 ───────────────────────────────────────────────────
print(f"\nUploading to s3://{BUCKET}/{S3_KEY} ...")
s3 = boto3.client("s3", region_name="us-east-1")

file_size = os.path.getsize(SAMPLE_FILE)
with tqdm(total=file_size, unit="B", unit_scale=True, desc="Uploading") as bar:
    s3.upload_file(
        SAMPLE_FILE,
        BUCKET,
        S3_KEY,
        Callback=lambda n: bar.update(n),
    )

obj = s3.head_object(Bucket=BUCKET, Key=S3_KEY)
print(f"\nIn S3: {obj['ContentLength'] / (1024*1024):.1f} MB")
print("Upload successful!")
