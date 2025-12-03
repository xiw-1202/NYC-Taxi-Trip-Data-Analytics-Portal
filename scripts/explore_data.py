import duckdb
import pandas as pd

# Connect to DuckDB (in-memory for now)
con = duckdb.connect()

# Read the parquet file
print("Loading data...")
df = con.execute(
    """
    SELECT * FROM '/Users/sam/Documents/School/Emory/CS554_Database Systems/Project/NYC-Taxi-Trip-Data-Analytics-Portal/data/raw/yellow_tripdata_2025-01.parquet'
    LIMIT 10
"""
).df()

print("\n=== FIRST 10 ROWS ===")
print(df)

# Get column names and types
print("\n=== COLUMN NAMES AND TYPES ===")
schema = con.execute(
    """
    DESCRIBE SELECT * FROM '/Users/sam/Documents/School/Emory/CS554_Database Systems/Project/NYC-Taxi-Trip-Data-Analytics-Portal/data/raw/yellow_tripdata_2025-01.parquet'
"""
).df()
print(schema)

# Get basic statistics
print("\n=== TOTAL NUMBER OF ROWS ===")
count = con.execute(
    """
    SELECT COUNT(*) as total_trips 
    FROM '/Users/sam/Documents/School/Emory/CS554_Database Systems/Project/NYC-Taxi-Trip-Data-Analytics-Portal/data/raw/yellow_tripdata_2025-01.parquet'
"""
).fetchone()[0]
print(f"Total trips in January 2025: {count:,}")

# Get date range
print("\n=== DATE RANGE ===")
date_range = con.execute(
    """
    SELECT 
        MIN(tpep_pickup_datetime) as first_trip,
        MAX(tpep_pickup_datetime) as last_trip
    FROM '/Users/sam/Documents/School/Emory/CS554_Database Systems/Project/NYC-Taxi-Trip-Data-Analytics-Portal/data/raw/yellow_tripdata_2025-01.parquet'
"""
).df()
print(date_range)

con.close()
