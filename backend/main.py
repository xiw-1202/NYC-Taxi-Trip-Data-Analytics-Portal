"""
NYC Taxi Data Analytics - FastAPI Backend
=========================================
RESTful API for 8 analytical features
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
from pathlib import Path
from typing import List, Dict, Any
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="NYC Taxi Analytics API",
    description="Analytics API for NYC Taxi Trip Data (24M+ trips)",
    version="1.0.0"
)

# CORS middleware for frontend
# For development - allows local file access
# For production, use environment variable: ALLOWED_ORIGINS="http://yourdomain.com"
import os
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Database path
DB_PATH = Path(__file__).parent.parent / "data" / "taxi_analytics.duckdb"

def get_db_connection():
    """Get database connection"""
    try:
        return duckdb.connect(str(DB_PATH), read_only=True)
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "NYC Taxi Analytics API",
        "version": "1.0.0",
        "endpoints": {
            "/stats": "Database statistics",
            "/api/zones": "Feature 1: Zone Popularity",
            "/api/temporal": "Feature 2: Temporal Demand",
            "/api/fare-structure": "Feature 3: Fare Breakdown",
            "/api/airport": "Feature 4: Airport Trips",
            "/api/od-flows": "Feature 5: OD Flow Patterns",
            "/api/vendors": "Feature 6: Vendor Performance",
            "/api/payment-tipping": "Feature 7: Payment & Tipping",
            "/api/anomalies": "Feature 8: Anomaly Detection"
        }
    }

@app.get("/stats")
async def get_statistics():
    """Get overall database statistics"""
    con = get_db_connection()
    try:
        stats = con.execute("""
            SELECT
                total_trips,
                total_revenue,
                avg_fare,
                avg_distance,
                avg_duration / 60.0 as avg_duration_min,
                first_trip_date,
                last_trip_date
            FROM summary_statistics
        """).fetchone()

        return {
            "total_trips": stats[0],
            "total_revenue": round(stats[1], 2),
            "avg_fare": round(stats[2], 2),
            "avg_distance": round(stats[3], 2),
            "avg_duration_min": round(stats[4], 1),
            "date_range": {
                "start": str(stats[5]),
                "end": str(stats[6])
            }
        }
    finally:
        con.close()

# ============================================================
# FEATURE 1: ZONE POPULARITY RANKING
# ============================================================

@app.get("/api/zones/top-pickup")
async def get_top_pickup_zones(limit: int = 20):
    """Get top N pickup zones by trip count"""
    # Input validation - validates limit is safe integer
    if not isinstance(limit, int) or not (1 <= limit <= 1000):
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 1000")

    con = get_db_connection()
    try:
        # Note: LIMIT uses f-string since it's validated as safe integer (not user string input)
        result = con.execute(f"""
            SELECT
                l.zone,
                l.borough,
                z.pickup_count,
                ROUND(z.avg_fare, 2) as avg_fare,
                ROUND(z.avg_distance, 2) as avg_distance,
                ROUND(z.median_fare, 2) as median_fare
            FROM mv_zone_pickup z
            JOIN dim_location l ON z.location_id = l.location_id
            WHERE l.borough = 'Manhattan'
            ORDER BY z.pickup_count DESC
            LIMIT {limit}
        """).fetchall()

        return {
            "zones": [
                {
                    "zone": row[0],
                    "borough": row[1],
                    "pickup_count": row[2],
                    "avg_fare": row[3],
                    "avg_distance": row[4],
                    "median_fare": row[5]
                }
                for row in result
            ]
        }
    finally:
        con.close()

@app.get("/api/zones/top-dropoff")
async def get_top_dropoff_zones(limit: int = 20):
    """Get top N dropoff zones by trip count"""
    # Input validation - validates limit is safe integer
    if not isinstance(limit, int) or not (1 <= limit <= 1000):
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 1000")

    con = get_db_connection()
    try:
        # Note: LIMIT uses f-string since it's validated as safe integer (not user string input)
        result = con.execute(f"""
            SELECT
                l.zone,
                l.borough,
                z.dropoff_count,
                ROUND(z.avg_fare, 2) as avg_fare,
                ROUND(z.avg_distance, 2) as avg_distance
            FROM mv_zone_dropoff z
            JOIN dim_location l ON z.location_id = l.location_id
            WHERE l.borough = 'Manhattan'
            ORDER BY z.dropoff_count DESC
            LIMIT {limit}
        """).fetchall()

        return {
            "zones": [
                {
                    "zone": row[0],
                    "borough": row[1],
                    "dropoff_count": row[2],
                    "avg_fare": row[3],
                    "avg_distance": row[4]
                }
                for row in result
            ]
        }
    finally:
        con.close()

# ============================================================
# FEATURE 2: TEMPORAL DEMAND CALENDAR
# ============================================================

@app.get("/api/temporal/hourly")
async def get_hourly_demand():
    """Get trip demand by hour of day"""
    con = get_db_connection()
    try:
        result = con.execute("""
            SELECT
                pickup_hour,
                SUM(trip_count) as total_trips,
                ROUND(AVG(avg_fare), 2) as avg_fare,
                ROUND(SUM(total_revenue), 2) as total_revenue
            FROM mv_hourly_demand
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """).fetchall()

        return {
            "hourly_data": [
                {
                    "hour": row[0],
                    "total_trips": row[1],
                    "avg_fare": row[2],
                    "total_revenue": row[3]
                }
                for row in result
            ]
        }
    finally:
        con.close()

@app.get("/api/temporal/day-of-week")
async def get_day_of_week_demand():
    """Get trip demand by day of week"""
    con = get_db_connection()
    try:
        result = con.execute("""
            SELECT
                pickup_day_of_week,
                CASE pickup_day_of_week
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END as day_name,
                is_weekend,
                SUM(trip_count) as total_trips,
                ROUND(AVG(avg_fare), 2) as avg_fare
            FROM mv_hourly_demand
            GROUP BY pickup_day_of_week, is_weekend
            ORDER BY pickup_day_of_week
        """).fetchall()

        return {
            "day_of_week_data": [
                {
                    "day_number": row[0],
                    "day_name": row[1],
                    "is_weekend": row[2],
                    "total_trips": row[3],
                    "avg_fare": row[4]
                }
                for row in result
            ]
        }
    finally:
        con.close()

@app.get("/api/temporal/heatmap")
async def get_temporal_heatmap():
    """Get hour x day_of_week heatmap data"""
    con = get_db_connection()
    try:
        result = con.execute("""
            SELECT
                pickup_day_of_week,
                pickup_hour,
                SUM(trip_count) as trip_count
            FROM mv_hourly_demand
            GROUP BY pickup_day_of_week, pickup_hour
            ORDER BY pickup_day_of_week, pickup_hour
        """).fetchall()

        return {
            "heatmap_data": [
                {
                    "day": row[0],
                    "hour": row[1],
                    "trips": row[2]
                }
                for row in result
            ]
        }
    finally:
        con.close()

# ============================================================
# FEATURE 3: FARE STRUCTURE BREAKDOWN
# ============================================================

@app.get("/api/fare-structure/breakdown")
async def get_fare_breakdown():
    """Get average fare component breakdown"""
    con = get_db_connection()
    try:
        result = con.execute("""
            SELECT
                COUNT(*) as total_trips,
                ROUND(AVG(fare_amount), 2) as avg_base_fare,
                ROUND(AVG(extra), 2) as avg_extra,
                ROUND(AVG(mta_tax), 2) as avg_mta_tax,
                ROUND(AVG(tip_amount), 2) as avg_tip,
                ROUND(AVG(tolls_amount), 2) as avg_tolls,
                ROUND(AVG(improvement_surcharge), 2) as avg_improvement_surcharge,
                ROUND(AVG(congestion_surcharge), 2) as avg_congestion_surcharge,
                ROUND(AVG(airport_fee), 2) as avg_airport_fee,
                ROUND(AVG(total_amount), 2) as avg_total,
                ROUND(AVG(tip_amount / NULLIF(total_amount, 0) * 100), 2) as tip_pct_of_total
            FROM fact_trip
            WHERE fare_amount > 0 AND fare_amount < 500
        """).fetchone()

        return {
            "total_trips": result[0],
            "components": {
                "base_fare": result[1],
                "extra": result[2],
                "mta_tax": result[3],
                "tip": result[4],
                "tolls": result[5],
                "improvement_surcharge": result[6],
                "congestion_surcharge": result[7],
                "airport_fee": result[8],
                "total": result[9]
            },
            "tip_percentage_of_total": result[10]
        }
    finally:
        con.close()

@app.get("/api/fare-structure/surcharges")
async def get_surcharge_frequency():
    """Get frequency of different surcharges"""
    con = get_db_connection()
    try:
        result = con.execute("""
            SELECT
                COUNT(*) as total_trips,
                SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) as trips_with_congestion,
                SUM(CASE WHEN airport_fee > 0 THEN 1 ELSE 0 END) as trips_with_airport_fee,
                SUM(CASE WHEN tolls_amount > 0 THEN 1 ELSE 0 END) as trips_with_tolls,
                ROUND(SUM(CASE WHEN congestion_surcharge > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as congestion_pct,
                ROUND(SUM(CASE WHEN airport_fee > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as airport_fee_pct,
                ROUND(SUM(CASE WHEN tolls_amount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as tolls_pct
            FROM fact_trip
            WHERE fare_amount > 0 AND fare_amount < 500
        """).fetchone()

        return {
            "total_trips": result[0],
            "surcharge_counts": {
                "congestion": result[1],
                "airport_fee": result[2],
                "tolls": result[3]
            },
            "surcharge_percentages": {
                "congestion": result[4],
                "airport_fee": result[5],
                "tolls": result[6]
            }
        }
    finally:
        con.close()

# ============================================================
# FEATURE 4: AIRPORT TRIP ANALYSIS
# ============================================================

@app.get("/api/airport/comparison")
async def get_airport_comparison():
    """Compare airport vs regular trips"""
    con = get_db_connection()
    try:
        result = con.execute("""
            SELECT
                CASE
                    WHEN rate_code_id = 2 THEN 'JFK Airport'
                    WHEN rate_code_id = 3 THEN 'Newark Airport'
                    WHEN airport_fee > 0 THEN 'Other Airport'
                    ELSE 'Regular Trip'
                END as trip_type,
                COUNT(*) as trip_count,
                ROUND(AVG(fare_amount), 2) as avg_fare,
                ROUND(AVG(trip_distance), 2) as avg_distance,
                ROUND(AVG(trip_duration_seconds / 60.0), 1) as avg_duration_min,
                ROUND(AVG(tip_amount), 2) as avg_tip
            FROM fact_trip
            WHERE fare_amount > 0 AND fare_amount < 1000
            GROUP BY trip_type
            ORDER BY trip_count DESC
        """).fetchall()

        return {
            "comparison": [
                {
                    "trip_type": row[0],
                    "trip_count": row[1],
                    "avg_fare": row[2],
                    "avg_distance": row[3],
                    "avg_duration_min": row[4],
                    "avg_tip": row[5]
                }
                for row in result
            ]
        }
    finally:
        con.close()

@app.get("/api/airport/top-origins")
async def get_top_airport_origins(limit: int = 20):
    """Get top origin zones for airport trips"""
    # Input validation - validates limit is safe integer
    if not isinstance(limit, int) or not (1 <= limit <= 1000):
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 1000")

    con = get_db_connection()
    try:
        # Note: LIMIT uses f-string since it's validated as safe integer (not user string input)
        result = con.execute(f"""
            SELECT
                l.zone,
                COUNT(*) as airport_trip_count,
                ROUND(AVG(t.fare_amount), 2) as avg_fare,
                ROUND(AVG(t.trip_distance), 2) as avg_distance
            FROM fact_trip t
            JOIN dim_location l ON t.pu_location_id = l.location_id
            WHERE l.borough = 'Manhattan'
              AND (t.rate_code_id IN (2, 3) OR t.airport_fee > 0)
              AND t.fare_amount > 0
            GROUP BY l.zone
            HAVING COUNT(*) >= 10
            ORDER BY airport_trip_count DESC
            LIMIT {limit}
        """).fetchall()

        return {
            "top_origins": [
                {
                    "zone": row[0],
                    "trip_count": row[1],
                    "avg_fare": row[2],
                    "avg_distance": row[3]
                }
                for row in result
            ]
        }
    finally:
        con.close()

# ============================================================
# FEATURE 5: ORIGIN-DESTINATION FLOW PATTERNS
# ============================================================

@app.get("/api/od-flows/top-routes")
async def get_top_routes(limit: int = 50):
    """Get top OD pairs by trip count"""
    # Input validation - validates limit is safe integer
    if not isinstance(limit, int) or not (1 <= limit <= 1000):
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 1000")

    con = get_db_connection()
    try:
        # Note: LIMIT uses f-string since it's validated as safe integer (not user string input)
        # Note: Changed alias from 'do' to 'dropoff' because 'do' is a reserved keyword in DuckDB
        result = con.execute(f"""
            SELECT
                pu.zone as origin_zone,
                dropoff.zone as destination_zone,
                od.trip_count,
                ROUND(od.avg_fare, 2) as avg_fare,
                ROUND(od.avg_distance, 2) as avg_distance,
                ROUND(od.avg_duration_sec / 60.0, 1) as avg_duration_min
            FROM mv_od_flows od
            JOIN dim_location pu ON od.pu_location_id = pu.location_id
            JOIN dim_location dropoff ON od.do_location_id = dropoff.location_id
            WHERE pu.borough = 'Manhattan' AND dropoff.borough = 'Manhattan'
            ORDER BY od.trip_count DESC
            LIMIT {limit}
        """).fetchall()

        return {
            "top_routes": [
                {
                    "origin": row[0],
                    "destination": row[1],
                    "trip_count": row[2],
                    "avg_fare": row[3],
                    "avg_distance": row[4],
                    "avg_duration_min": row[5]
                }
                for row in result
            ]
        }
    finally:
        con.close()

# ============================================================
# FEATURE 6: VENDOR MARKET SHARE & PERFORMANCE
# ============================================================

@app.get("/api/vendors/performance")
async def get_vendor_performance():
    """Get vendor performance metrics"""
    con = get_db_connection()
    try:
        result = con.execute("""
            SELECT
                v.vendor_name,
                v.vendor_short_name,
                vp.trip_count,
                ROUND(vp.trip_count * 100.0 / SUM(vp.trip_count) OVER (), 2) as market_share_pct,
                ROUND(vp.avg_fare, 2) as avg_fare,
                ROUND(vp.avg_tip, 2) as avg_tip,
                ROUND(vp.avg_distance, 2) as avg_distance,
                ROUND(vp.avg_duration_sec / 60.0, 1) as avg_duration_min
            FROM mv_vendor_performance vp
            JOIN dim_vendor v ON vp.vendor_id = v.vendor_id
            ORDER BY vp.trip_count DESC
        """).fetchall()

        return {
            "vendors": [
                {
                    "vendor_name": row[0],
                    "vendor_short": row[1],
                    "trip_count": row[2],
                    "market_share_pct": row[3],
                    "avg_fare": row[4],
                    "avg_tip": row[5],
                    "avg_distance": row[6],
                    "avg_duration_min": row[7]
                }
                for row in result
            ]
        }
    finally:
        con.close()

# ============================================================
# FEATURE 7: PAYMENT METHOD & TIPPING BEHAVIOR
# ============================================================

@app.get("/api/payment-tipping/by-payment-type")
async def get_tipping_by_payment():
    """Get tipping behavior by payment type"""
    con = get_db_connection()
    try:
        result = con.execute("""
            SELECT
                pt.payment_type_name,
                pt.is_card_payment,
                pt.allows_tip,
                COUNT(*) as trip_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_trips,
                ROUND(AVG(t.tip_amount), 2) as avg_tip,
                ROUND(AVG(t.fare_amount), 2) as avg_fare,
                ROUND(AVG(CASE WHEN t.fare_amount > 0 THEN (t.tip_amount / t.fare_amount) * 100 ELSE NULL END), 2) as avg_tip_pct,
                ROUND(SUM(CASE WHEN t.tip_amount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as tipping_frequency_pct
            FROM fact_trip t
            JOIN dim_payment_type pt ON t.payment_type_id = pt.payment_type_id
            WHERE t.fare_amount > 0 AND t.fare_amount < 500
            GROUP BY pt.payment_type_name, pt.is_card_payment, pt.allows_tip
            ORDER BY trip_count DESC
        """).fetchall()

        return {
            "payment_types": [
                {
                    "payment_type": row[0],
                    "is_card": row[1],
                    "allows_tip": row[2],
                    "trip_count": row[3],
                    "pct_of_trips": row[4],
                    "avg_tip": row[5],
                    "avg_fare": row[6],
                    "avg_tip_pct": row[7],
                    "tipping_frequency_pct": row[8]
                }
                for row in result
            ]
        }
    finally:
        con.close()

# ============================================================
# FEATURE 8: OUTLIER & ANOMALY DETECTION
# ============================================================

@app.get("/api/anomalies/high-fare-per-mile")
async def get_high_fare_per_mile(limit: int = 100):
    """Get trips with unusually high fare per mile"""
    # Input validation - validates limit is safe integer
    if not isinstance(limit, int) or not (1 <= limit <= 1000):
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 1000")

    con = get_db_connection()
    try:
        # Note: LIMIT uses f-string since it's validated as safe integer (not user string input)
        # Note: Changed alias from 'do' to 'dropoff' because 'do' is a reserved keyword in DuckDB
        result = con.execute(f"""
            SELECT
                t.trip_id,
                t.pickup_datetime,
                pu.zone as pickup_zone,
                dropoff.zone as dropoff_zone,
                ROUND(t.fare_amount, 2) as fare_amount,
                ROUND(t.trip_distance, 2) as trip_distance,
                ROUND(t.fare_amount / NULLIF(t.trip_distance, 0), 2) as fare_per_mile,
                ROUND(t.trip_duration_seconds / 60.0, 1) as duration_min
            FROM fact_trip t
            JOIN dim_location pu ON t.pu_location_id = pu.location_id
            JOIN dim_location dropoff ON t.do_location_id = dropoff.location_id
            WHERE t.trip_distance > 0
              AND t.trip_distance < 100
              AND t.fare_amount / t.trip_distance > 50
              AND t.fare_amount < 1000
            ORDER BY fare_per_mile DESC
            LIMIT {limit}
        """).fetchall()

        return {
            "anomalies": [
                {
                    "trip_id": row[0],
                    "pickup_datetime": str(row[1]),
                    "pickup_zone": row[2],
                    "dropoff_zone": row[3],
                    "fare_amount": row[4],
                    "trip_distance": row[5],
                    "fare_per_mile": row[6],
                    "duration_min": row[7]
                }
                for row in result
            ]
        }
    finally:
        con.close()

@app.get("/api/anomalies/summary")
async def get_anomaly_summary():
    """Get summary statistics of different anomaly types"""
    con = get_db_connection()
    try:
        # High fare per mile
        high_fare = con.execute("""
            SELECT COUNT(*)
            FROM fact_trip
            WHERE trip_distance > 0
              AND fare_amount / trip_distance > 50
              AND fare_amount < 1000
        """).fetchone()[0]

        # No tip on expensive trips
        no_tip = con.execute("""
            SELECT COUNT(*)
            FROM fact_trip t
            JOIN dim_payment_type pt ON t.payment_type_id = pt.payment_type_id
            WHERE t.fare_amount > 50
              AND t.tip_amount = 0
              AND pt.allows_tip = TRUE
        """).fetchone()[0]

        # Total trips
        total = con.execute("SELECT COUNT(*) FROM fact_trip").fetchone()[0]

        return {
            "total_trips": total,
            "anomalies": {
                "high_fare_per_mile": {
                    "count": high_fare,
                    "percentage": round(high_fare * 100.0 / total, 2)
                },
                "no_tip_expensive": {
                    "count": no_tip,
                    "percentage": round(no_tip * 100.0 / total, 2)
                }
            }
        }
    finally:
        con.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
