#!/bin/bash

# PHANTOM Start Script
# This script ensures the server starts from the correct directory with the right environment.

# Get the directory where this script is located
PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_ROOT"

echo "Starting PHANTOM Orchestrator from $PROJECT_ROOT"

# Check if virtual environment exists
if [ -f "venv/bin/activate" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
else
    echo "Warning: venv not found. Using system python."
fi

# Run the server
# We use -m uvicorn server.main:app to ensure the 'server' package is correctly handled
python3 -m uvicorn server.main:app --host 0.0.0.0 --port 8000 --reload
