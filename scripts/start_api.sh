#!/bin/bash
# Start the Workflow Engine API server

echo "Starting Workflow Engine API..."

cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "workflow_venv" ]; then
    echo "Activating virtual environment..."
    source workflow_venv/bin/activate
fi

# Start API server
echo "Starting uvicorn on port 8000..."
uvicorn api.server:app --host 0.0.0.0 --port 8000


