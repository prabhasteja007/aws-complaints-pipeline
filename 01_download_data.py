import requests
import os
from tqdm import tqdm

URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "complaints.csv.zip")

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Downloading CFPB Consumer Complaint Database...")
print(f"Source: {URL}\n")

response = requests.get(URL, stream=True)
total_size = int(response.headers.get("content-length", 0))

with open(OUTPUT_FILE, "wb") as f, tqdm(
    desc="Downloading",
    total=total_size,
    unit="B",
    unit_scale=True,
    unit_divisor=1024,
) as bar:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)
        bar.update(len(chunk))

print(f"\nSaved to {OUTPUT_FILE}")

# Unzip
import zipfile
print("Unzipping...")
with zipfile.ZipFile(OUTPUT_FILE, "r") as z:
    z.extractall(OUTPUT_DIR)

print("Done. Files in data/:")
for f in os.listdir(OUTPUT_DIR):
    size_mb = os.path.getsize(os.path.join(OUTPUT_DIR, f)) / (1024 * 1024)
    print(f"  {f}  ({size_mb:.1f} MB)")
