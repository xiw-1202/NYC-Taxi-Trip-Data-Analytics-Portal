-- ============================================================
-- NYC TAXI DATA ANALYTICS - DuckDB Compatible Schema
-- ============================================================
-- DuckDB version without unsupported features
-- ============================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_trip;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_vendor;
DROP TABLE IF EXISTS dim_payment_type;
DROP TABLE IF EXISTS dim_rate_code;

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE dim_location (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR(50) NOT NULL,
    zone VARCHAR(100) NOT NULL,
    service_zone VARCHAR(50)
);

CREATE TABLE dim_vendor (
    vendor_id INTEGER PRIMARY KEY,
    vendor_name VARCHAR(100) NOT NULL,
    vendor_short_name VARCHAR(10) NOT NULL
);

CREATE TABLE dim_payment_type (
    payment_type_id INTEGER PRIMARY KEY,
    payment_type_name VARCHAR(50) NOT NULL,
    is_card_payment BOOLEAN NOT NULL,
    allows_tip BOOLEAN NOT NULL
);

CREATE TABLE dim_rate_code (
    rate_code_id INTEGER PRIMARY KEY,
    rate_code_name VARCHAR(100) NOT NULL,
    is_airport BOOLEAN NOT NULL,
    is_standard BOOLEAN NOT NULL
);

-- ============================================================
-- FACT TABLE
-- ============================================================

CREATE TABLE fact_trip (
    -- Primary key
    trip_id BIGINT PRIMARY KEY,

    -- Foreign keys to dimensions
    vendor_id INTEGER NOT NULL,
    pu_location_id INTEGER NOT NULL,
    do_location_id INTEGER NOT NULL,
    payment_type_id INTEGER NOT NULL,
    rate_code_id INTEGER NOT NULL,

    -- Temporal attributes
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pickup_date DATE NOT NULL,
    pickup_hour TINYINT NOT NULL,
    pickup_day_of_week TINYINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,

    -- Trip characteristics
    passenger_count TINYINT,
    trip_distance FLOAT NOT NULL,
    trip_duration_seconds INTEGER NOT NULL,

    -- Fare breakdown
    fare_amount FLOAT NOT NULL,
    extra FLOAT DEFAULT 0,
    mta_tax FLOAT DEFAULT 0,
    tip_amount FLOAT DEFAULT 0,
    tolls_amount FLOAT DEFAULT 0,
    improvement_surcharge FLOAT DEFAULT 0,
    total_amount FLOAT NOT NULL,
    congestion_surcharge FLOAT DEFAULT 0,
    airport_fee FLOAT DEFAULT 0,
    cbd_congestion_fee FLOAT DEFAULT 0,

    -- Additional flags
    store_and_fwd_flag VARCHAR(1)
);

-- ============================================================
-- INDEXES FOR QUERY OPTIMIZATION
-- ============================================================

-- Temporal indexes
CREATE INDEX idx_pickup_datetime ON fact_trip(pickup_datetime);
CREATE INDEX idx_pickup_date ON fact_trip(pickup_date);
CREATE INDEX idx_pickup_hour ON fact_trip(pickup_hour);

-- Spatial indexes
CREATE INDEX idx_pu_location ON fact_trip(pu_location_id);
CREATE INDEX idx_do_location ON fact_trip(do_location_id);

-- Composite index for OD pair analysis
CREATE INDEX idx_od_pair ON fact_trip(pu_location_id, do_location_id);

-- Dimensional indexes
CREATE INDEX idx_vendor ON fact_trip(vendor_id);
CREATE INDEX idx_payment_type ON fact_trip(payment_type_id);
CREATE INDEX idx_rate_code ON fact_trip(rate_code_id);

-- Composite for temporal-spatial queries
CREATE INDEX idx_zone_temporal ON fact_trip(pu_location_id, pickup_date);
