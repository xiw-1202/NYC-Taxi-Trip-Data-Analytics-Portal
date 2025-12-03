#!/usr/bin/env python3
"""
Download NYC Taxi Trip Data
Downloads all required data files for the analytics project
"""

import os
import requests

# Create data directory if it doesn't exist
os.makedirs("./data/raw", exist_ok=True)

print("=" * 70)
print(" NYC Taxi Data Download Script")
print("=" * 70)
print()

# Base URLs
trip_data_base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
misc_base = "https://d37ci6vzurychx.cloudfront.net/misc"

# Download taxi zone lookup table
print("1. Downloading Taxi Zone Lookup Table...")
print("-" * 70)

zone_filename = "taxi_zone_lookup.csv"
zone_url = f"{misc_base}/{zone_filename}"
zone_filepath = f"./data/raw/{zone_filename}"

if os.path.exists(zone_filepath):
    print(f"✓ {zone_filename} already exists, skipping...")
else:
    try:
        print(f"Downloading {zone_filename}...")
        response = requests.get(zone_url)
        response.raise_for_status()

        with open(zone_filepath, "wb") as f:
            f.write(response.content)

        size_kb = len(response.content) / 1024
        print(f"✓ {zone_filename} downloaded successfully! ({size_kb:.1f} KB)")
    except Exception as e:
        print(f"✗ Error downloading {zone_filename}: {e}")

print()

# Download yellow taxi trip data
print("2. Downloading Yellow Taxi Trip Data (10 months)...")
print("-" * 70)

months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]
year = "2025"

for i, month in enumerate(months, 1):
    filename = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"{trip_data_base}/{filename}"
    filepath = f"./data/raw/{filename}"

    # Check if already exists
    if os.path.exists(filepath):
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"✓ [{i:2d}/10] {filename:45s} {size_mb:6.1f} MB (exists)")
        continue

    print(f"  [{i:2d}/10] Downloading {filename}...")
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
                    mb_downloaded = downloaded / (1024 * 1024)
                    mb_total = total_size / (1024 * 1024)
                    print(f"          Progress: {percent:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)", end="\r")

        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"  ✓ [{i:2d}/10] {filename:45s} {size_mb:6.1f} MB")

    except Exception as e:
        print(f"  ✗ [{i:2d}/10] Error downloading {filename}: {e}")

print()
print("=" * 70)
print(" Download Summary")
print("=" * 70)

# List all downloaded files
import glob

# Check zone lookup
if os.path.exists(zone_filepath):
    size_kb = os.path.getsize(zone_filepath) / 1024
    print(f"✓ {zone_filename:45s} {size_kb:6.1f} KB")
else:
    print(f"✗ {zone_filename:45s} MISSING")

print()

# Check parquet files
parquet_files = glob.glob("./data/raw/yellow_tripdata_2025-*.parquet")
parquet_files.sort()

total_size = 0
missing_months = []

for month in months:
    filename = f"yellow_tripdata_2025-{month}.parquet"
    filepath = f"./data/raw/{filename}"

    if os.path.exists(filepath):
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        total_size += size_mb
        print(f"✓ {filename:45s} {size_mb:6.1f} MB")
    else:
        print(f"✗ {filename:45s} MISSING")
        missing_months.append(month)

print("=" * 70)
print(f"Parquet files: {len(parquet_files)}/10")
print(f"Total size: {total_size:.1f} MB")

if len(parquet_files) == 10 and os.path.exists(zone_filepath):
    print()
    print("✓ All required files downloaded successfully!")
    print()
    print("Next steps:")
    print("  1. Run ETL pipeline: python3 etl_pipeline.py")
    print("  2. Start backend: ./run_backend.sh")
    print("  3. Open frontend: open frontend/index.html")
else:
    print()
    print("✗ Some files are missing. Please re-run this script.")
    if missing_months:
        print(f"   Missing months: {', '.join(missing_months)}")

print("=" * 70)
