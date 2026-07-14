import pandas as pd
import boto3
import os
from tqdm import tqdm

RAW_FILE = "data/complaints.csv"
PARQUET_FILE = "data/complaints_500k.parquet"
BUCKET = "complaints-pipeline-teja-ohio"
S3_KEY = "raw/complaints_500k.parquet"

# ── Step 1: Read 500K rows and save as Parquet ─────────────────────────────
print("Reading 500,000 rows...")
df = pd.read_csv(RAW_FILE, nrows=50_000, low_memory=False)
print(f"Loaded: {len(df):,} rows | {len(df.columns)} columns")

print("Converting to Parquet...")
df.to_parquet(PARQUET_FILE, index=False, compression="snappy")

csv_mb = os.path.getsize("data/complaints_500k.csv") / (1024 * 1024)
parquet_mb = os.path.getsize(PARQUET_FILE) / (1024 * 1024)
print(f"CSV size:     {csv_mb:.1f} MB")
print(f"Parquet size: {parquet_mb:.1f} MB  ({csv_mb/parquet_mb:.1f}x compression)")

# ── Step 2: Upload Parquet to S3 ───────────────────────────────────────────
print(f"\nUploading {parquet_mb:.1f} MB to s3://{BUCKET}/{S3_KEY} ...")
s3 = boto3.client("s3", region_name="us-east-2")

file_size = os.path.getsize(PARQUET_FILE)
with tqdm(total=file_size, unit="B", unit_scale=True, desc="Uploading") as bar:
    s3.upload_file(
        PARQUET_FILE,
        BUCKET,
        S3_KEY,
        Callback=lambda n: bar.update(n),
    )

obj = s3.head_object(Bucket=BUCKET, Key=S3_KEY)
print(f"\nIn S3: {obj['ContentLength'] / (1024*1024):.1f} MB")
print("Upload successful!")
