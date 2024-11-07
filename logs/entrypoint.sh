#!/bin/bash
set -e

# Generate sample log file
echo "Generating sample log file..."
python generator.py

# Prepare database
echo "Preparing database..."
python app/prepare_db.py

# Process logs and populate database
echo "Processing logs..."
python app/log_processor.py

# Start FastAPI server
echo "Starting API server..."
uvicorn app.api:app --host 0.0.0.0 --port 8000