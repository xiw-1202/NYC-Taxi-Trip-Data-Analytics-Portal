#!/usr/bin/env python3
"""
NYC Taxi Data Analytics - ETL Pipeline
======================================
Efficient data loading pipeline for 29M Manhattan taxi trips
Features:
- Batch processing of 10 months Parquet files
- Manhattan-only filtering
- Data quality validation
- Incremental loading with progress tracking
- Materialized view creation
- Index building with optimization
"""

import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import sys

# Configuration - use relative paths for portability
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DB_PATH = PROJECT_ROOT / "data" / "taxi_analytics.duckdb"
ZONE_LOOKUP_PATH = DATA_DIR / "taxi_zone_lookup.csv"

# Performance settings
BATCH_SIZE = 1_000_000  # Process 1M rows at a time
USE_PARALLEL = True
MEMORY_LIMIT = '8GB'

class TaxiETLPipeline:
    """ETL Pipeline for NYC Taxi Data"""

    def __init__(self, db_path):
        """Initialize database connection"""
        print(f"[{self._timestamp()}] Initializing ETL Pipeline...")

        # Create database connection with optimizations
        self.con = duckdb.connect(str(db_path))

        # Set DuckDB configuration for performance
        self.con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        self.con.execute("SET threads=8")  # Use 8 threads
        self.con.execute("SET preserve_insertion_order=false")  # Faster inserts

        print(f"[{self._timestamp()}] Database initialized at: {db_path}")
        print(f"[{self._timestamp()}] Configuration: {MEMORY_LIMIT} memory, 8 threads")

    def _timestamp(self):
        """Get current timestamp string"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def create_schema(self):
        """Create database schema with DuckDB-compatible structure"""
        print(f"\n[{self._timestamp()}] Creating database schema...")

        # Read and execute the DuckDB-compatible schema
        schema_path = Path(__file__).parent / "database" / "schema_duckdb.sql"

        if schema_path.exists():
            print(f"[{self._timestamp()}] Loading schema from: {schema_path}")
            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            # Execute schema - split by semicolon and filter comments
            statements = []
            for stmt in schema_sql.split(';'):
                stmt = stmt.strip()
                # Remove SQL comments
                lines = [line for line in stmt.split('\n') if not line.strip().startswith('--')]
                clean_stmt = '\n'.join(lines).strip()
                if clean_stmt:
                    statements.append(clean_stmt)

            print(f"[{self._timestamp()}] Executing {len(statements)} SQL statements...")
            for i, stmt in enumerate(statements):
                try:
                    self.con.execute(stmt)
                    if (i + 1) % 5 == 0:
                        print(f"[{self._timestamp()}] Progress: {i+1}/{len(statements)} statements")
                except Exception as e:
                    print(f"[{self._timestamp()}] Error in statement {i+1}: {str(e)[:150]}")
                    raise
        else:
            print(f"[{self._timestamp()}] Schema file not found, creating basic schema...")
            self._create_basic_schema()

        print(f"[{self._timestamp()}] Schema created successfully")

    def _create_basic_schema(self):
        """Create basic schema if optimized version not available"""
        # Dimension tables
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS dim_location (
                location_id INTEGER PRIMARY KEY,
                borough VARCHAR(50),
                zone VARCHAR(100),
                service_zone VARCHAR(50)
            )
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS dim_vendor (
                vendor_id INTEGER PRIMARY KEY,
                vendor_name VARCHAR(100),
                vendor_short_name VARCHAR(10)
            )
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS dim_payment_type (
                payment_type_id INTEGER PRIMARY KEY,
                payment_type_name VARCHAR(50),
                is_card_payment BOOLEAN,
                allows_tip BOOLEAN
            )
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS dim_rate_code (
                rate_code_id INTEGER PRIMARY KEY,
                rate_code_name VARCHAR(100),
                is_airport BOOLEAN,
                is_standard BOOLEAN
            )
        """)

        # Fact table
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS fact_trip (
                trip_id BIGINT PRIMARY KEY,
                vendor_id INTEGER,
                pu_location_id INTEGER,
                do_location_id INTEGER,
                payment_type_id INTEGER,
                rate_code_id INTEGER,
                pickup_datetime TIMESTAMP,
                dropoff_datetime TIMESTAMP,
                pickup_date DATE,
                pickup_hour TINYINT,
                pickup_day_of_week TINYINT,
                is_weekend BOOLEAN,
                passenger_count TINYINT,
                trip_distance FLOAT,
                trip_duration_seconds INTEGER,
                fare_amount FLOAT,
                extra FLOAT,
                mta_tax FLOAT,
                tip_amount FLOAT,
                tolls_amount FLOAT,
                improvement_surcharge FLOAT,
                total_amount FLOAT,
                congestion_surcharge FLOAT,
                airport_fee FLOAT,
                cbd_congestion_fee FLOAT,
                store_and_fwd_flag VARCHAR(1)
            )
        """)

    def load_dimensions(self):
        """Load dimension tables"""
        print(f"\n[{self._timestamp()}] Loading dimension tables...")

        # Load location dimension from CSV
        print(f"[{self._timestamp()}] Loading taxi zones...")
        zones_df = pd.read_csv(ZONE_LOOKUP_PATH)

        # Rename columns to match schema (lowercase)
        zones_df.columns = [col.lower() for col in zones_df.columns]

        # Rename to match schema exactly
        zones_df = zones_df.rename(columns={
            'locationid': 'location_id'
        })

        # Drop rows with null values in required columns
        initial_count = len(zones_df)
        zones_df = zones_df.dropna(subset=['location_id', 'borough', 'zone'])
        filtered_count = initial_count - len(zones_df)
        if filtered_count > 0:
            print(f"[{self._timestamp()}] Filtered {filtered_count} zones with null values")

        # Fill null service_zone with 'Unknown'
        zones_df['service_zone'] = zones_df['service_zone'].fillna('Unknown')

        # Insert into dim_location
        self.con.execute("INSERT INTO dim_location SELECT * FROM zones_df")

        zone_count = self.con.execute("SELECT COUNT(*) FROM dim_location").fetchone()[0]
        manhattan_count = self.con.execute(
            "SELECT COUNT(*) FROM dim_location WHERE borough = 'Manhattan'"
        ).fetchone()[0]

        print(f"[{self._timestamp()}] Loaded {zone_count} zones ({manhattan_count} Manhattan)")

        # Load vendor dimension
        print(f"[{self._timestamp()}] Loading vendors...")
        self.con.execute("""
            INSERT INTO dim_vendor VALUES
                (1, 'Creative Mobile Technologies', 'CMT'),
                (2, 'VeriFone Inc.', 'VTS'),
                (6, 'Other', 'Other'),
                (7, 'Other', 'Other')
        """)
        print(f"[{self._timestamp()}] Loaded vendors")

        # Load payment type dimension
        print(f"[{self._timestamp()}] Loading payment types...")
        self.con.execute("""
            INSERT INTO dim_payment_type VALUES
                (0, 'Unknown', FALSE, FALSE),
                (1, 'Credit card', TRUE, TRUE),
                (2, 'Cash', FALSE, FALSE),
                (3, 'No charge', FALSE, FALSE),
                (4, 'Dispute', FALSE, FALSE),
                (5, 'Unknown', FALSE, FALSE)
        """)
        print(f"[{self._timestamp()}] Loaded payment types")

        # Load rate code dimension
        print(f"[{self._timestamp()}] Loading rate codes...")
        self.con.execute("""
            INSERT INTO dim_rate_code VALUES
                (1, 'Standard rate', FALSE, TRUE),
                (2, 'JFK', TRUE, FALSE),
                (3, 'Newark', TRUE, FALSE),
                (4, 'Nassau or Westchester', FALSE, FALSE),
                (5, 'Negotiated fare', FALSE, FALSE),
                (6, 'Group ride', FALSE, FALSE),
                (99, 'Other', FALSE, FALSE)
        """)
        print(f"[{self._timestamp()}] Loaded rate codes")

    def load_trip_data(self):
        """Load trip data from Parquet files with filtering and transformation"""
        print(f"\n[{self._timestamp()}] Loading trip data...")

        # Get Manhattan zone IDs
        manhattan_zones = self.con.execute("""
            SELECT location_id FROM dim_location WHERE borough = 'Manhattan'
        """).fetchall()
        manhattan_zone_ids = [z[0] for z in manhattan_zones]

        print(f"[{self._timestamp()}] Manhattan zones: {len(manhattan_zone_ids)} IDs")

        # Find all parquet files
        parquet_files = sorted(DATA_DIR.glob("yellow_tripdata_2025-*.parquet"))

        if not parquet_files:
            print(f"[{self._timestamp()}] ERROR: No parquet files found in {DATA_DIR}")
            return

        print(f"[{self._timestamp()}] Found {len(parquet_files)} parquet files")

        total_trips_loaded = 0
        total_trips_filtered = 0

        # Process each file
        for i, parquet_file in enumerate(parquet_files, 1):
            print(f"\n[{self._timestamp()}] Processing file {i}/{len(parquet_files)}: {parquet_file.name}")

            start_time = time.time()

            try:
                # Load and filter data in one query (most efficient in DuckDB)
                insert_query = f"""
                    INSERT INTO fact_trip
                    SELECT
                        -- Generate unique trip_id
                        ROW_NUMBER() OVER () + {total_trips_loaded} as trip_id,

                        -- Foreign keys
                        VendorID as vendor_id,
                        PULocationID as pu_location_id,
                        DOLocationID as do_location_id,
                        payment_type as payment_type_id,
                        RatecodeID as rate_code_id,

                        -- Temporal attributes
                        tpep_pickup_datetime as pickup_datetime,
                        tpep_dropoff_datetime as dropoff_datetime,
                        CAST(tpep_pickup_datetime AS DATE) as pickup_date,
                        CAST(EXTRACT(HOUR FROM tpep_pickup_datetime) AS TINYINT) as pickup_hour,
                        CAST(EXTRACT(DOW FROM tpep_pickup_datetime) AS TINYINT) as pickup_day_of_week,
                        CASE WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,

                        -- Trip characteristics
                        CAST(passenger_count AS TINYINT) as passenger_count,
                        CAST(trip_distance AS FLOAT) as trip_distance,
                        CAST(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) AS INTEGER) as trip_duration_seconds,

                        -- Fare breakdown
                        CAST(fare_amount AS FLOAT) as fare_amount,
                        CAST(extra AS FLOAT) as extra,
                        CAST(mta_tax AS FLOAT) as mta_tax,
                        CAST(tip_amount AS FLOAT) as tip_amount,
                        CAST(tolls_amount AS FLOAT) as tolls_amount,
                        CAST(improvement_surcharge AS FLOAT) as improvement_surcharge,
                        CAST(total_amount AS FLOAT) as total_amount,
                        CAST(congestion_surcharge AS FLOAT) as congestion_surcharge,
                        CAST(airport_fee AS FLOAT) as airport_fee,
                        CAST(0 AS FLOAT) as cbd_congestion_fee,  -- Add if available in data

                        -- Flags
                        store_and_fwd_flag

                    FROM read_parquet('{parquet_file}')
                    WHERE
                        -- Manhattan-only trips (both pickup and dropoff)
                        PULocationID IN ({','.join(map(str, manhattan_zone_ids))})
                        AND DOLocationID IN ({','.join(map(str, manhattan_zone_ids))})

                        -- Data quality filters
                        AND fare_amount > 0
                        AND fare_amount < 500
                        AND trip_distance > 0
                        AND trip_distance < 200
                        AND passenger_count BETWEEN 1 AND 6
                        AND tpep_pickup_datetime < tpep_dropoff_datetime
                        AND EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) < 7200
                """

                # Execute insert
                self.con.execute(insert_query)

                # Get counts
                current_count = self.con.execute("SELECT COUNT(*) FROM fact_trip").fetchone()[0]
                trips_added = current_count - total_trips_loaded
                total_trips_loaded = current_count

                # Get original count from file
                original_count = self.con.execute(f"""
                    SELECT COUNT(*) FROM read_parquet('{parquet_file}')
                """).fetchone()[0]

                trips_filtered = original_count - trips_added
                total_trips_filtered += trips_filtered

                elapsed = time.time() - start_time
                rate = trips_added / elapsed if elapsed > 0 else 0

                print(f"[{self._timestamp()}] File processed in {elapsed:.1f}s")
                print(f"[{self._timestamp()}] Original: {original_count:,} → Loaded: {trips_added:,} → Filtered: {trips_filtered:,} ({trips_filtered*100/original_count:.1f}%)")
                print(f"[{self._timestamp()}] Loading rate: {rate:,.0f} trips/sec")
                print(f"[{self._timestamp()}] Total trips so far: {total_trips_loaded:,}")

            except Exception as e:
                print(f"[{self._timestamp()}] ERROR processing {parquet_file.name}: {str(e)}")
                continue

        print(f"\n[{self._timestamp()}] ========== DATA LOADING COMPLETE ==========")
        print(f"[{self._timestamp()}] Total trips loaded: {total_trips_loaded:,}")
        print(f"[{self._timestamp()}] Total trips filtered: {total_trips_filtered:,}")
        print(f"[{self._timestamp()}] Filter rate: {total_trips_filtered*100/(total_trips_loaded+total_trips_filtered):.1f}%")

    def create_indexes(self):
        """Create indexes for query optimization"""
        print(f"\n[{self._timestamp()}] Creating indexes...")

        indexes = [
            ("idx_pickup_datetime_btree", "fact_trip(pickup_datetime)"),
            ("idx_pu_location", "fact_trip(pu_location_id)"),
            ("idx_do_location", "fact_trip(do_location_id)"),
            ("idx_od_pair_covering", "fact_trip(pu_location_id, do_location_id)"),
            ("idx_vendor_analysis", "fact_trip(vendor_id)"),
            ("idx_payment_tipping", "fact_trip(payment_type_id, tip_amount)"),
            ("idx_zone_temporal", "fact_trip(pu_location_id, pickup_date, pickup_hour)"),
        ]

        for idx_name, idx_def in indexes:
            try:
                print(f"[{self._timestamp()}] Creating {idx_name}...")
                start_time = time.time()

                self.con.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_def}")

                elapsed = time.time() - start_time
                print(f"[{self._timestamp()}] Created {idx_name} in {elapsed:.1f}s")
            except Exception as e:
                print(f"[{self._timestamp()}] Warning: Failed to create {idx_name}: {str(e)[:100]}")

        print(f"[{self._timestamp()}] Indexes created successfully")

    def create_materialized_views(self):
        """Create materialized views for performance"""
        print(f"\n[{self._timestamp()}] Creating materialized views...")

        # MV 1: Zone Pickup Statistics
        print(f"[{self._timestamp()}] Creating mv_zone_pickup...")
        start_time = time.time()
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS mv_zone_pickup AS
            SELECT
                pu_location_id as location_id,
                COUNT(*) as pickup_count,
                AVG(fare_amount) as avg_fare,
                AVG(trip_distance) as avg_distance,
                AVG(trip_duration_seconds) as avg_duration,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fare_amount) as median_fare
            FROM fact_trip
            WHERE fare_amount > 0 AND fare_amount < 500
              AND trip_distance > 0
            GROUP BY pu_location_id
        """)
        print(f"[{self._timestamp()}] Created mv_zone_pickup in {time.time() - start_time:.1f}s")

        # MV 2: Zone Dropoff Statistics
        print(f"[{self._timestamp()}] Creating mv_zone_dropoff...")
        start_time = time.time()
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS mv_zone_dropoff AS
            SELECT
                do_location_id as location_id,
                COUNT(*) as dropoff_count,
                AVG(fare_amount) as avg_fare,
                AVG(trip_distance) as avg_distance
            FROM fact_trip
            WHERE fare_amount > 0 AND fare_amount < 500
              AND trip_distance > 0
            GROUP BY do_location_id
        """)
        print(f"[{self._timestamp()}] Created mv_zone_dropoff in {time.time() - start_time:.1f}s")

        # MV 3: Hourly Demand
        print(f"[{self._timestamp()}] Creating mv_hourly_demand...")
        start_time = time.time()
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS mv_hourly_demand AS
            SELECT
                pickup_date,
                pickup_hour,
                pickup_day_of_week,
                is_weekend,
                COUNT(*) as trip_count,
                AVG(fare_amount) as avg_fare,
                SUM(total_amount) as total_revenue,
                AVG(trip_distance) as avg_distance,
                AVG(passenger_count) as avg_passengers
            FROM fact_trip
            WHERE fare_amount > 0
            GROUP BY pickup_date, pickup_hour, pickup_day_of_week, is_weekend
        """)
        print(f"[{self._timestamp()}] Created mv_hourly_demand in {time.time() - start_time:.1f}s")

        # MV 4: OD Flows (high volume routes only)
        print(f"[{self._timestamp()}] Creating mv_od_flows...")
        start_time = time.time()
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS mv_od_flows AS
            SELECT
                pu_location_id,
                do_location_id,
                COUNT(*) as trip_count,
                AVG(fare_amount) as avg_fare,
                AVG(trip_distance) as avg_distance,
                AVG(trip_duration_seconds) as avg_duration_sec,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fare_amount) as median_fare,
                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY fare_amount) as p90_fare
            FROM fact_trip
            WHERE fare_amount > 0
              AND fare_amount < 500
              AND trip_distance > 0
            GROUP BY pu_location_id, do_location_id
            HAVING COUNT(*) >= 100
        """)
        print(f"[{self._timestamp()}] Created mv_od_flows in {time.time() - start_time:.1f}s")

        # MV 5: Vendor Performance
        print(f"[{self._timestamp()}] Creating mv_vendor_performance...")
        start_time = time.time()
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS mv_vendor_performance AS
            SELECT
                vendor_id,
                COUNT(*) as trip_count,
                AVG(fare_amount) as avg_fare,
                AVG(tip_amount) as avg_tip,
                AVG(trip_distance) as avg_distance,
                AVG(trip_duration_seconds) as avg_duration_sec,
                SUM(CASE WHEN store_and_fwd_flag = 'Y' THEN 1 ELSE 0 END) as store_fwd_count,
                AVG(passenger_count) as avg_passengers
            FROM fact_trip
            WHERE fare_amount > 0
            GROUP BY vendor_id
        """)
        print(f"[{self._timestamp()}] Created mv_vendor_performance in {time.time() - start_time:.1f}s")

        # MV 6: Payment Patterns
        print(f"[{self._timestamp()}] Creating mv_payment_patterns...")
        start_time = time.time()
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS mv_payment_patterns AS
            SELECT
                payment_type_id,
                pickup_hour,
                is_weekend,
                COUNT(*) as trip_count,
                AVG(tip_amount) as avg_tip,
                AVG(fare_amount) as avg_fare,
                AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100 ELSE NULL END) as avg_tip_pct,
                SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) as trips_with_tip,
                COUNT(*) - SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) as trips_no_tip
            FROM fact_trip
            WHERE fare_amount > 0 AND fare_amount < 500
            GROUP BY payment_type_id, pickup_hour, is_weekend
        """)
        print(f"[{self._timestamp()}] Created mv_payment_patterns in {time.time() - start_time:.1f}s")

        print(f"[{self._timestamp()}] All materialized views created successfully")

    def create_summary_statistics(self):
        """Create overall summary statistics"""
        print(f"\n[{self._timestamp()}] Creating summary statistics...")

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS summary_statistics AS
            SELECT
                'overall' as metric_type,
                COUNT(*) as total_trips,
                SUM(fare_amount) as total_fare_amount,
                SUM(tip_amount) as total_tips,
                SUM(total_amount) as total_revenue,
                AVG(fare_amount) as avg_fare,
                AVG(trip_distance) as avg_distance,
                AVG(trip_duration_seconds) as avg_duration,
                AVG(passenger_count) as avg_passengers,
                MIN(pickup_datetime) as first_trip_date,
                MAX(pickup_datetime) as last_trip_date
            FROM fact_trip
            WHERE fare_amount > 0
        """)

        stats = self.con.execute("SELECT * FROM summary_statistics").fetchone()
        print(f"[{self._timestamp()}] Summary statistics created")
        print(f"[{self._timestamp()}] Total trips: {stats[1]:,}")
        print(f"[{self._timestamp()}] Total revenue: ${stats[4]:,.2f}")
        print(f"[{self._timestamp()}] Avg fare: ${stats[5]:.2f}")
        print(f"[{self._timestamp()}] Date range: {stats[9]} to {stats[10]}")

    def analyze_database(self):
        """Run ANALYZE to update statistics for query optimizer"""
        print(f"\n[{self._timestamp()}] Analyzing database for query optimization...")

        try:
            self.con.execute("ANALYZE")
            print(f"[{self._timestamp()}] Database analysis complete")
        except:
            print(f"[{self._timestamp()}] Note: ANALYZE not available in this DuckDB version")

    def print_statistics(self):
        """Print final database statistics"""
        print(f"\n[{self._timestamp()}] ========== DATABASE STATISTICS ==========")

        # Table sizes
        tables = ['fact_trip', 'dim_location', 'dim_vendor', 'dim_payment_type', 'dim_rate_code',
                  'mv_zone_pickup', 'mv_zone_dropoff', 'mv_hourly_demand', 'mv_od_flows',
                  'mv_vendor_performance', 'mv_payment_patterns']

        for table in tables:
            try:
                count = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"[{self._timestamp()}] {table}: {count:,} rows")
            except:
                print(f"[{self._timestamp()}] {table}: not found")

        # Data quality check
        print(f"\n[{self._timestamp()}] Data Quality:")
        quality = self.con.execute("""
            SELECT
                COUNT(*) as total,
                AVG(fare_amount) as avg_fare,
                AVG(trip_distance) as avg_dist,
                AVG(trip_duration_seconds / 60.0) as avg_duration_min
            FROM fact_trip
        """).fetchone()

        print(f"[{self._timestamp()}] Average fare: ${quality[1]:.2f}")
        print(f"[{self._timestamp()}] Average distance: {quality[2]:.2f} miles")
        print(f"[{self._timestamp()}] Average duration: {quality[3]:.1f} minutes")

    def close(self):
        """Close database connection"""
        if hasattr(self, 'con'):
            self.con.close()
            print(f"\n[{self._timestamp()}] Database connection closed")


def main():
    """Main ETL execution"""
    print("="*70)
    print(" NYC TAXI DATA ANALYTICS - ETL PIPELINE")
    print("="*70)

    start_time = time.time()

    try:
        # Initialize pipeline
        pipeline = TaxiETLPipeline(DB_PATH)

        # Step 1: Create schema
        pipeline.create_schema()

        # Step 2: Load dimensions
        pipeline.load_dimensions()

        # Step 3: Load trip data
        pipeline.load_trip_data()

        # Step 4: Create indexes
        pipeline.create_indexes()

        # Step 5: Create materialized views
        pipeline.create_materialized_views()

        # Step 6: Create summary statistics
        pipeline.create_summary_statistics()

        # Step 7: Analyze for optimization
        pipeline.analyze_database()

        # Step 8: Print statistics
        pipeline.print_statistics()

        # Cleanup
        pipeline.close()

        total_time = time.time() - start_time
        print(f"\n{'='*70}")
        print(f" ETL PIPELINE COMPLETED SUCCESSFULLY")
        print(f" Total time: {total_time/60:.1f} minutes ({total_time:.0f} seconds)")
        print(f" Database: {DB_PATH}")
        print(f"{'='*70}")

    except Exception as e:
        print(f"\n{'='*70}")
        print(f" ETL PIPELINE FAILED")
        print(f" Error: {str(e)}")
        print(f"{'='*70}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
