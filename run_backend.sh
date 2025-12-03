#!/bin/bash
# Script to start the FastAPI backend server

echo "🚕 NYC Taxi Analytics - Starting Backend API"
echo "=============================================="
echo ""

# Check if database exists
if [ ! -f "data/taxi_analytics.duckdb" ]; then
    echo "❌ Error: Database not found!"
    echo "Please run: python3 etl_pipeline.py first"
    exit 1
fi

# Check if dependencies are installed
python3 -c "import fastapi, uvicorn" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "📦 Installing dependencies..."
    pip3 install -r requirements.txt
fi

# Start backend
cd backend
echo "✅ Starting API server on http://localhost:8000"
echo ""
echo "API Documentation: http://localhost:8000/docs"
echo "Press Ctrl+C to stop"
echo ""

python3 main.py
