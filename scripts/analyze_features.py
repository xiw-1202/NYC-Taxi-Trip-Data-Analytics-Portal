import duckdb

con = duckdb.connect()

# Use a variable for the path (easier to maintain)
DATA_PATH = "./data/raw/yellow_tripdata_2025-01.parquet"

print("=" * 60)
print("NYC TAXI DATA ANALYSIS - JANUARY 2025")
print("=" * 60)

# 1. Check Manhattan trips (LocationID filtering)
print("\n1. LOCATION ANALYSIS")
print("-" * 60)
manhattan_check = con.execute(
    f"""
    SELECT 
        COUNT(*) as total_trips,
        COUNT(DISTINCT PULocationID) as unique_pickup_zones,
        COUNT(DISTINCT DOLocationID) as unique_dropoff_zones
    FROM '{DATA_PATH}'
"""
).df()
print(manhattan_check)

# 2. Vendor distribution
print("\n2. VENDOR DISTRIBUTION")
print("-" * 60)
vendor_dist = con.execute(
    f"""
    SELECT 
        VendorID,
        COUNT(*) as trip_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM '{DATA_PATH}'
    GROUP BY VendorID
    ORDER BY trip_count DESC
"""
).df()
print(vendor_dist)

# 3. Payment type distribution
print("\n3. PAYMENT TYPE DISTRIBUTION")
print("-" * 60)
payment_dist = con.execute(
    f"""
    SELECT 
        payment_type,
        COUNT(*) as trip_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM '{DATA_PATH}'
    GROUP BY payment_type
    ORDER BY trip_count DESC
"""
).df()
print(payment_dist)

# 4. Rate code distribution
print("\n4. RATE CODE DISTRIBUTION")
print("-" * 60)
rate_dist = con.execute(
    f"""
    SELECT 
        RatecodeID,
        COUNT(*) as trip_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM '{DATA_PATH}'
    GROUP BY RatecodeID
    ORDER BY trip_count DESC
"""
).df()
print(rate_dist)

# 5. Passenger count distribution
print("\n5. PASSENGER COUNT DISTRIBUTION")
print("-" * 60)
passenger_dist = con.execute(
    f"""
    SELECT 
        passenger_count,
        COUNT(*) as trip_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM '{DATA_PATH}'
    GROUP BY passenger_count
    ORDER BY passenger_count
"""
).df()
print(passenger_dist)

# 6. Fare statistics
print("\n6. FARE STATISTICS")
print("-" * 60)
fare_stats = con.execute(
    f"""
    SELECT 
        ROUND(AVG(fare_amount), 2) as avg_fare,
        ROUND(MIN(fare_amount), 2) as min_fare,
        ROUND(MAX(fare_amount), 2) as max_fare,
        ROUND(AVG(tip_amount), 2) as avg_tip,
        ROUND(AVG(trip_distance), 2) as avg_distance
    FROM '{DATA_PATH}'
    WHERE fare_amount > 0 AND fare_amount < 500
"""
).df()
print(fare_stats)

# 7. Temporal patterns (hourly)
print("\n7. TRIPS BY HOUR OF DAY")
print("-" * 60)
hourly_dist = con.execute(
    f"""
    SELECT 
        EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
        COUNT(*) as trip_count
    FROM '{DATA_PATH}'
    GROUP BY hour
    ORDER BY hour
"""
).df()
print(hourly_dist)

# 8. Top 10 pickup locations
print("\n8. TOP 10 PICKUP LOCATIONS")
print("-" * 60)
top_pickup = con.execute(
    f"""
    SELECT 
        PULocationID,
        COUNT(*) as trip_count
    FROM '{DATA_PATH}'
    GROUP BY PULocationID
    ORDER BY trip_count DESC
    LIMIT 10
"""
).df()
print(top_pickup)

# 9. Top 10 dropoff locations
print("\n9. TOP 10 DROPOFF LOCATIONS")
print("-" * 60)
top_dropoff = con.execute(
    f"""
    SELECT 
        DOLocationID,
        COUNT(*) as trip_count
    FROM '{DATA_PATH}'
    GROUP BY DOLocationID
    ORDER BY trip_count DESC
    LIMIT 10
"""
).df()
print(top_dropoff)

# 10. Data quality check
print("\n10. DATA QUALITY CHECK")
print("-" * 60)
quality_check = con.execute(
    f"""
    SELECT 
        COUNT(*) as total_trips,
        SUM(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END) as invalid_fares,
        SUM(CASE WHEN trip_distance <= 0 THEN 1 ELSE 0 END) as invalid_distance,
        SUM(CASE WHEN passenger_count <= 0 OR passenger_count > 6 THEN 1 ELSE 0 END) as invalid_passengers,
        SUM(CASE WHEN PULocationID IS NULL OR DOLocationID IS NULL THEN 1 ELSE 0 END) as null_locations
    FROM '{DATA_PATH}'
"""
).df()
print(quality_check)

con.close()
print("\n" + "=" * 60)
print("ANALYSIS COMPLETE!")
print("=" * 60)
