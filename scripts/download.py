import os
import requests

# Create data directory if it doesn't exist
os.makedirs("./data/raw", exist_ok=True)

# Months to download (we already have January)
months = ["02", "03", "04", "05", "06", "07", "08", "09", "10"]
year = "2025"

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

for month in months:
    filename = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"{base_url}/{filename}"
    filepath = f"./data/raw/{filename}"

    # Check if already exists
    if os.path.exists(filepath):
        print(f"✓ {filename} already exists, skipping...")
        continue

    print(f"Downloading {filename}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Get file size
        total_size = int(response.headers.get("content-length", 0))

        with open(filepath, "wb") as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                # Show progress
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    print(f"  Progress: {percent:.1f}%", end="\r")

        print(f"✓ {filename} downloaded successfully!")

    except Exception as e:
        print(f"✗ Error downloading {filename}: {e}")

print("\n" + "=" * 60)
print("Download complete! Checking files...")
print("=" * 60)

# List all downloaded files
import glob

files = glob.glob("./data/raw/yellow_tripdata_2025-*.parquet")
files.sort()

total_size = 0
for f in files:
    size_mb = os.path.getsize(f) / (1024 * 1024)
    total_size += size_mb
    print(f"✓ {os.path.basename(f):40s} {size_mb:6.1f} MB")

print("=" * 60)
print(f"Total files: {len(files)}")
print(f"Total size: {total_size:.1f} MB")
