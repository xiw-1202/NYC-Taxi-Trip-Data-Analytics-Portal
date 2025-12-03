#!/usr/bin/env python3
"""
Download NYC Taxi Trip Data
Downloads Parquet files for the analytics project
Note: taxi_zone_lookup.csv is already included in the repository
"""

import os
import requests

# Create data directory if it doesn't exist
os.makedirs("./data/raw", exist_ok=True)

print("=" * 70)
print(" NYC Taxi Data Download Script")
print("=" * 70)
print()

# Check if zone lookup file exists (should be in repo)
zone_filepath = "./data/raw/taxi_zone_lookup.csv"
if os.path.exists(zone_filepath):
    size_kb = os.path.getsize(zone_filepath) / 1024
    print(f"✓ taxi_zone_lookup.csv found ({size_kb:.1f} KB)")
else:
    print("⚠️  Warning: taxi_zone_lookup.csv not found!")
    print("   This file should be included in the repository.")

print()

# Download yellow taxi trip data
print("Downloading Yellow Taxi Trip Data (10 months)...")
print("-" * 70)

trip_data_base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
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

# Check all required files
import glob

# Check zone lookup
if os.path.exists(zone_filepath):
    size_kb = os.path.getsize(zone_filepath) / 1024
    print(f"✓ taxi_zone_lookup.csv                          {size_kb:6.1f} KB")
else:
    print(f"✗ taxi_zone_lookup.csv                          MISSING")

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
print(f"Total data size: {total_size:.1f} MB")

if len(parquet_files) == 10 and os.path.exists(zone_filepath):
    print()
    print("✓ All required files present!")
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
    if not os.path.exists(zone_filepath):
        print(f"   Missing: taxi_zone_lookup.csv (should be in repository)")

print("=" * 70)
