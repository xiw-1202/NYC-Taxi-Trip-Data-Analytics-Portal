import pandas as pd
import duckdb

# Load the zone lookup
print("Loading taxi zone lookup...")
zone_df = pd.read_csv("./data/raw/taxi_zone_lookup.csv")

print("\n=== ZONE DATA OVERVIEW ===")
print(f"Total zones: {len(zone_df)}")
print(f"\nColumns: {zone_df.columns.tolist()}")

print("\n=== FIRST 10 ZONES ===")
print(zone_df.head(10))

print("\n=== BOROUGH DISTRIBUTION ===")
print(zone_df["Borough"].value_counts())

print("\n=== MANHATTAN ZONES ===")
manhattan_zones = zone_df[zone_df["Borough"] == "Manhattan"]
print(f"Total Manhattan zones: {len(manhattan_zones)}")
print("\nAll Manhattan zones:")
print(manhattan_zones[["LocationID", "Borough", "Zone"]].to_string())

# Now let's see what percentage of trips are Manhattan
con = duckdb.connect()

DATA_PATH = "./data/raw/yellow_tripdata_2025-01.parquet"

# Get Manhattan LocationIDs as a list
manhattan_ids = manhattan_zones["LocationID"].tolist()

print("\n" + "=" * 60)
print("MANHATTAN TRIP ANALYSIS")
print("=" * 60)

# Check pickup in Manhattan
manhattan_pickup = con.execute(
    f"""
    SELECT 
        COUNT(*) as total_trips,
        SUM(CASE WHEN PULocationID IN ({','.join(map(str, manhattan_ids))}) THEN 1 ELSE 0 END) as manhattan_pickup,
        ROUND(SUM(CASE WHEN PULocationID IN ({','.join(map(str, manhattan_ids))}) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pickup_percentage
    FROM '{DATA_PATH}'
"""
).df()
print("\nPickup in Manhattan:")
print(manhattan_pickup)

# Check dropoff in Manhattan
manhattan_dropoff = con.execute(
    f"""
    SELECT 
        COUNT(*) as total_trips,
        SUM(CASE WHEN DOLocationID IN ({','.join(map(str, manhattan_ids))}) THEN 1 ELSE 0 END) as manhattan_dropoff,
        ROUND(SUM(CASE WHEN DOLocationID IN ({','.join(map(str, manhattan_ids))}) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as dropoff_percentage
    FROM '{DATA_PATH}'
"""
).df()
print("\nDropoff in Manhattan:")
print(manhattan_dropoff)

# Both pickup AND dropoff in Manhattan
both_manhattan = con.execute(
    f"""
    SELECT 
        COUNT(*) as both_manhattan,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM '{DATA_PATH}'), 2) as percentage
    FROM '{DATA_PATH}'
    WHERE PULocationID IN ({','.join(map(str, manhattan_ids))})
      AND DOLocationID IN ({','.join(map(str, manhattan_ids))})
"""
).df()
print("\nBoth pickup AND dropoff in Manhattan:")
print(both_manhattan)

# Either pickup OR dropoff in Manhattan
either_manhattan = con.execute(
    f"""
    SELECT 
        COUNT(*) as either_manhattan,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM '{DATA_PATH}'), 2) as percentage
    FROM '{DATA_PATH}'
    WHERE PULocationID IN ({','.join(map(str, manhattan_ids))})
       OR DOLocationID IN ({','.join(map(str, manhattan_ids))})
"""
).df()
print("\nEither pickup OR dropoff in Manhattan:")
print(either_manhattan)

# Top 10 Manhattan-to-Manhattan routes
print("\n=== TOP 10 MANHATTAN-TO-MANHATTAN ROUTES ===")
top_routes = con.execute(
    f"""
    SELECT 
        PULocationID,
        DOLocationID,
        COUNT(*) as trip_count
    FROM '{DATA_PATH}'
    WHERE PULocationID IN ({','.join(map(str, manhattan_ids))})
      AND DOLocationID IN ({','.join(map(str, manhattan_ids))})
    GROUP BY PULocationID, DOLocationID
    ORDER BY trip_count DESC
    LIMIT 10
"""
).df()
print(top_routes)

con.close()

print("\n" + "=" * 60)
print("ZONE EXPLORATION COMPLETE!")
print("=" * 60)
