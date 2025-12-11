# NYC Taxi Trip Data Analytics Portal

**CS 554 - Database Systems | Fall 2025**
**Team:** Yiming Cheng, Xiaofei Wang, Andy Wu

---

## 📋 Project Overview

An interactive analytics portal demonstrating advanced database design, query optimization, and data analysis using NYC Taxi & Limousine Commission (TLC) Trip Record Data.

**Dataset:** 10 months (Jan-Oct 2025), ~24 million Manhattan taxi trips
**Database:** DuckDB (column-store OLAP database)
**Backend:** Python FastAPI with 16 optimized REST endpoints
**Frontend:** Single-page web application with interactive visualizations

---

## 🎯 8 Analytical Features

Each feature demonstrates different SQL techniques and query optimization strategies:

1. **Zone Popularity Ranking** - Aggregation with GROUP BY and ORDER BY
2. **Temporal Demand Analysis** - Time-series queries with date/time functions
3. **Fare Structure Breakdown** - Statistical aggregations and percentages
4. **Airport Trip Analysis** - Complex JOINs with spatial filtering
5. **Origin-Destination Flow Patterns** - Multi-table JOINs with composite keys
6. **Vendor Market Share** - Window functions and comparative analysis
7. **Payment & Tipping Behavior** - Conditional aggregations and behavioral analytics
8. **Outlier & Anomaly Detection** - Percentile functions and statistical analysis

### 🆕 New Features

**Unified Tabbed Interface:**
- All 8 analytical features are now consolidated into a single unified view
- Easy navigation between features using tab buttons
- Clean, modern UI with responsive design

**Region Comparison Tool:**
- Compare metrics between two zones side-by-side
- Available for Zone Popularity, Airport Trips, and OD Flow Patterns
- Interactive zone selectors with auto-populated lists
- Detailed comparison tables showing:
  - Individual metrics for each zone
  - Calculated differences with color-coded indicators
  - Key performance indicators (KPIs)
- Real-time comparison results displayed after clicking "Compare Zones" button

---

## 📊 Database Schema Design

### Star Schema Architecture

Our database uses a **star schema** optimized for OLAP (Online Analytical Processing) queries. This design separates dimensional data from fact data for efficient querying and analysis.

```
         dim_vendor (3 vendors)
                |
                |
         dim_rate_code (7 rate types)
                |
                |
    dim_payment_type (5 types) -----> FACT_TRIP (24M rows) <---- dim_location (265 zones)
                                              |
                                              |
                                      Temporal attributes
                                    (date, hour, day_of_week)
```

### Fact Table: `fact_trip`

**Size:** ~24 million rows, ~6 GB
**Purpose:** Stores individual trip transactions with foreign keys to dimension tables

**Key Columns:**
- `trip_id` (BIGINT, PRIMARY KEY) - Unique trip identifier
- `vendor_id`, `pu_location_id`, `do_location_id`, `payment_type_id`, `rate_code_id` (FOREIGN KEYS)
- `pickup_datetime`, `dropoff_datetime` (TIMESTAMP) - Trip timestamps
- `pickup_date`, `pickup_hour`, `pickup_day_of_week` (Denormalized for query performance)
- `trip_distance`, `trip_duration_seconds` - Trip characteristics
- `fare_amount`, `tip_amount`, `total_amount` - Fare breakdown
- `congestion_surcharge`, `airport_fee`, `tolls_amount` - Surcharge details

### Dimension Tables

**`dim_location`** (265 zones, 69 in Manhattan)
- `location_id` (INTEGER, PRIMARY KEY)
- `borough`, `zone`, `service_zone` (VARCHAR)
- Enables spatial filtering and zone-based aggregations

**`dim_vendor`** (3 vendors)
- `vendor_id` (INTEGER, PRIMARY KEY)
- `vendor_name`, `vendor_short_name` (VARCHAR)
- CMT, VeriFone, and other taxi vendors

**`dim_payment_type`** (5 types)
- `payment_type_id` (INTEGER, PRIMARY KEY)
- `payment_type_name`, `is_card_payment`, `allows_tip` (VARCHAR, BOOLEAN)
- Credit card, Cash, No charge, Dispute, Unknown

**`dim_rate_code`** (7 rate types)
- `rate_code_id` (INTEGER, PRIMARY KEY)
- `rate_code_name`, `is_airport`, `is_standard` (VARCHAR, BOOLEAN)
- Standard, JFK, Newark, Nassau/Westchester, Negotiated, Group ride, Unknown

---

## ⚡ Query Optimization Strategies

### 1. Indexing Strategy (10 indexes)

**Temporal Indexes** (for time-based queries):
```sql
CREATE INDEX idx_pickup_datetime ON fact_trip(pickup_datetime);
CREATE INDEX idx_pickup_date ON fact_trip(pickup_date);
CREATE INDEX idx_pickup_hour ON fact_trip(pickup_hour);
```

**Spatial Indexes** (for zone-based queries):
```sql
CREATE INDEX idx_pu_location ON fact_trip(pu_location_id);
CREATE INDEX idx_do_location ON fact_trip(do_location_id);
```

**Composite Indexes** (for multi-column queries):
```sql
CREATE INDEX idx_od_pair ON fact_trip(pu_location_id, do_location_id);
CREATE INDEX idx_zone_temporal ON fact_trip(pu_location_id, pickup_date);
```

**Dimensional Indexes**:
```sql
CREATE INDEX idx_vendor ON fact_trip(vendor_id);
CREATE INDEX idx_payment_type ON fact_trip(payment_type_id);
CREATE INDEX idx_rate_code ON fact_trip(rate_code_id);
```

### 2. Query Design Patterns

**Pattern 1: Aggregation with JOINs** (Zone Popularity)
```sql
SELECT
    l.zone,
    COUNT(*) as trip_count,
    AVG(f.fare_amount) as avg_fare
FROM fact_trip f
JOIN dim_location l ON f.pu_location_id = l.location_id
WHERE l.borough = 'Manhattan'
GROUP BY l.zone
ORDER BY trip_count DESC
LIMIT 10;
```

**Pattern 2: Temporal Analysis** (Hourly Demand)
```sql
SELECT
    pickup_hour,
    COUNT(*) as trip_count,
    AVG(trip_distance) as avg_distance
FROM fact_trip
WHERE pickup_date >= '2025-01-01'
GROUP BY pickup_hour
ORDER BY pickup_hour;
```

**Pattern 3: Multi-table JOINs** (Origin-Destination Flows)
```sql
SELECT
    pu.zone as origin,
    dropoff.zone as destination,
    COUNT(*) as trip_count,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.trip_duration_seconds / 60.0) as avg_duration_min
FROM fact_trip f
JOIN dim_location pu ON f.pu_location_id = pu.location_id
JOIN dim_location dropoff ON f.do_location_id = dropoff.location_id
WHERE pu.borough = 'Manhattan' AND dropoff.borough = 'Manhattan'
GROUP BY pu.zone, dropoff.zone
ORDER BY trip_count DESC
LIMIT 100;
```

**Pattern 4: Statistical Analysis** (Anomaly Detection)
```sql
SELECT
    trip_id,
    pu.zone,
    fare_amount,
    trip_distance,
    (fare_amount / NULLIF(trip_distance, 0)) as fare_per_mile
FROM fact_trip f
JOIN dim_location pu ON f.pu_location_id = pu.location_id
WHERE trip_distance > 0.5
    AND fare_amount > 0
    AND (fare_amount / trip_distance) > 50
ORDER BY fare_per_mile DESC
LIMIT 100;
```

**Pattern 5: Window Functions** (Vendor Comparison)
```sql
SELECT
    v.vendor_name,
    COUNT(*) as trip_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as market_share_pct
FROM fact_trip f
JOIN dim_vendor v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name;
```

### 3. DuckDB-Specific Optimizations

**Column-Store Advantages:**
- Columnar storage reduces I/O for analytical queries
- Efficient compression (~6 GB for 24M rows with indexes)
- Vectorized query execution for fast aggregations

**Query Performance:**
- Simple aggregations: <100ms
- Complex multi-table JOINs: 200-500ms
- Full table scans with filters: 500-1000ms

---

## 🚀 Quick Start Guide

### Prerequisites
- Python 3.8+
- Git

### 1. Clone Repository

```bash
git clone https://github.com/xiw-1202/NYC-Taxi-Trip-Data-Analytics-Portal.git
cd NYC-Taxi-Trip-Data-Analytics-Portal
```

### 2. Install Dependencies

```bash
# Create virtual environment (optional but recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
```

**Required packages:**
- `duckdb` - Column-store database engine
- `pandas` - Data manipulation
- `pyarrow` - Parquet file reading
- `fastapi` - REST API framework
- `uvicorn` - ASGI server

### 3. Download Data Files

**Note:** Parquet files are too large for GitHub (~650 MB). The zone lookup CSV is already included in the repository.

#### Option 1: Automated Download (Recommended)

```bash
# Download all Parquet files automatically
python3 scripts/download.py
```

This will download:
- 10 Parquet files (Jan-Oct 2025): `yellow_tripdata_2025-01.parquet` through `yellow_tripdata_2025-10.parquet`
- Total download: ~650 MB
- Zone lookup CSV (`taxi_zone_lookup.csv`) is already in `data/raw/` from the repository

#### Option 2: Manual Download

Visit the [NYC TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and download:

**Yellow Taxi Trip Records** (Parquet format):
- January 2025: `yellow_tripdata_2025-01.parquet`
- February 2025: `yellow_tripdata_2025-02.parquet`
- March 2025: `yellow_tripdata_2025-03.parquet`
- April 2025: `yellow_tripdata_2025-04.parquet`
- May 2025: `yellow_tripdata_2025-05.parquet`
- June 2025: `yellow_tripdata_2025-06.parquet`
- July 2025: `yellow_tripdata_2025-07.parquet`
- August 2025: `yellow_tripdata_2025-08.parquet`
- September 2025: `yellow_tripdata_2025-09.parquet`
- October 2025: `yellow_tripdata_2025-10.parquet`

Place all Parquet files in the `data/raw/` directory. The `taxi_zone_lookup.csv` is already there from the repository.

### 4. Build Database (First Time Only)

```bash
# Run ETL pipeline to create database from Parquet files
python3 etl_pipeline.py
```

**ETL Process:**
1. Loads 10 months of taxi trip data from `data/raw/` (Parquet format)
2. Creates star schema with fact and dimension tables
3. Filters for Manhattan trips only
4. Creates 10 indexes for query optimization
5. Generates `data/taxi_analytics.duckdb` (~6 GB)

**Time:** ~4-5 minutes on modern hardware

**Verify Data Files:**
```bash
# Check that all files are present
ls -lh data/raw/
# Should show 10 .parquet files and taxi_zone_lookup.csv
```

### 5. Start Backend Server

```bash
# Option 1: Using shell script
./run_backend.sh

# Option 2: Manual start
cd backend
python3 main.py
```

Backend will start on `http://localhost:8000`

**Available endpoints:**
- `GET /` - API information
- `GET /stats` - Database statistics
- `GET /api/zones/top-pickup` - Top pickup zones
- `GET /api/zones/top-dropoff` - Top dropoff zones
- `GET /api/temporal/hourly` - Hourly demand patterns
- `GET /api/temporal/day-of-week` - Day of week patterns
- `GET /api/temporal/heatmap` - Hour × Day heatmap
- `GET /api/fare-structure/breakdown` - Fare component analysis
- `GET /api/fare-structure/surcharges` - Surcharge statistics
- `GET /api/airport/comparison` - Airport trip comparison
- `GET /api/airport/top-origins` - Top airport origins
- `GET /api/od-flows/top-routes` - Top OD routes
- `GET /api/vendors/performance` - Vendor comparison
- `GET /api/payment-tipping/by-payment-type` - Payment analysis
- `GET /api/anomalies/high-fare-per-mile` - Fare anomalies
- `GET /api/anomalies/summary` - Anomaly summary

### 6. Open Frontend

```bash
# Open in default browser
open frontend/index.html

# Or manually navigate to:
# file:///path/to/NYC-Taxi-Trip-Data-Analytics-Portal/frontend/index.html
```

The frontend will connect to `http://localhost:8000` and display 8 interactive analytics features.

### 🎨 Frontend Features

**Unified Interface:**
- **Tab Navigation:** Switch between 8 analytical features using tab buttons at the top
- **Responsive Design:** Works on desktop and tablet devices
- **Interactive Charts:** Powered by Plotly.js for dynamic visualizations

**Region Comparison:**
- **Zone Popularity Comparison:** Compare pickup counts, average fares, distances, and median fares between two zones
- **Airport Trip Comparison:** Analyze airport trip volumes, fares, and distances by origin zone
- **OD Flow Comparison:** Compare route patterns, trip counts, and efficiency metrics between origin zones
- **Auto-populated Lists:** Zone selectors automatically populate with available zones from the dataset
- **Visual Indicators:** Color-coded differences (green for higher, red for lower values)
- **Real-time Results:** Comparison tables appear instantly after clicking the compare button

---

## 📦 Data Files

**What's included in the repository:**
- ✅ `taxi_zone_lookup.csv` (12 KB) - Zone reference data

**What needs to be downloaded:**
- ❌ 10 Parquet files (~650 MB) - Trip data for Jan-Oct 2025
- ❌ Generated DuckDB database (~6 GB) - Created by ETL pipeline

**Why Parquet files not included?**
- Individual files: 56-74 MB each
- Total: ~650 MB compressed
- Exceeds GitHub's 100 MB file size limit for individual files

**Download options:**
- **Automated:** `python3 scripts/download.py` (recommended)
- **Manual:** Download from [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

**After cloning:**
1. Repository includes: Code, schema, zone lookup CSV
2. You download: 10 Parquet files (~650 MB)
3. ETL pipeline creates: DuckDB database (~6 GB)

---

## 📁 Project Structure

```
NYC-Taxi-Trip-Data-Analytics-Portal/
├── README.md                        # This file
├── requirements.txt                 # Python dependencies
├── etl_pipeline.py                 # ETL pipeline script
├── run_backend.sh                  # Backend startup script
│
├── backend/
│   └── main.py                     # FastAPI REST API (16 endpoints)
│
├── frontend/
│   └── index.html                  # Single-page web application with tabbed interface and region comparison
│
├── database/
│   ├── schema_duckdb.sql          # Complete database schema
│   └── queries.sql                # SQL queries
│
├── data/
│   ├── raw/                       # Source Parquet files (10 months)
│   └── taxi_analytics.duckdb      # Generated database file
│
└── scripts/
    ├── download.py                # Data download utility
    ├── explore_data.py            # Data exploration
    ├── explore_zones.py           # Zone analysis
    └── analyze_features.py        # Feature analysis
```

---

## 📈 Database Statistics

**Dataset Coverage:**
- Time period: January - October 2025
- Total trips: ~24 million (Manhattan only)
- Data size: 10 Parquet files, ~581.6 MB raw

**Database Size:**
- DuckDB file: ~6 GB
- Includes: 24M rows + 10 indexes + dimension tables

**Table Sizes:**
- `fact_trip`: 24,118,902 rows
- `dim_location`: 265 rows (69 Manhattan zones)
- `dim_vendor`: 3 rows
- `dim_payment_type`: 5 rows
- `dim_rate_code`: 7 rows

**Data Quality:**
- Zero null locations (enforced by ETL)
- Filtered invalid fares (<$0)
- Filtered invalid distances (<0)
- Manhattan trips only (both pickup and dropoff)

---

## 🎓 Database Concepts Demonstrated

### Schema Design
✅ Star schema for OLAP workloads
✅ Fact table with foreign keys to dimensions
✅ Denormalization for query performance (temporal attributes)
✅ Appropriate data types for storage efficiency

### Indexing
✅ B-tree indexes on high-cardinality columns
✅ Composite indexes for multi-column queries
✅ Strategic index placement based on query patterns
✅ Trade-offs between query speed and storage

### Query Optimization
✅ JOINs with indexed columns
✅ Aggregate functions (COUNT, AVG, SUM)
✅ Window functions for comparative analysis
✅ Subqueries and Common Table Expressions (CTEs)
✅ Filtering at multiple levels (WHERE, HAVING)

### Advanced SQL
✅ Percentile functions for statistical analysis
✅ CASE statements for conditional logic
✅ String functions for data formatting
✅ Date/time functions for temporal analysis
✅ NULL handling and data validation

### Column-Store Benefits
✅ Efficient compression (~4.5:1 ratio)
✅ Fast aggregations on columnar data
✅ Vectorized query execution
✅ Reduced I/O for analytical queries

---

## 📞 Contact

**Team Members:**
- Yiming Cheng
- Xiaofei Wang
- Andy Wu

**Course:** CS 554 - Database Systems
**Institution:** Emory University
**Semester:** Fall 2025
