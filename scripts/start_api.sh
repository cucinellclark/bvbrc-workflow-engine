#!/bin/bash
# Start the Workflow Engine API server

echo "Starting Workflow Engine API..."

cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "workflow_venv" ]; then
    echo "Activating virtual environment..."
    source workflow_venv/bin/activate
fi

# Start API server using main.py
# Use environment variables if set, otherwise use config defaults
if [ -n "$API_HOST" ] && [ -n "$API_PORT" ]; then
    echo "Starting API server on $API_HOST:$API_PORT..."
    python main.py --host "$API_HOST" --port "$API_PORT"
else
    echo "Starting API server (using config defaults)..."
    python main.py
fi


