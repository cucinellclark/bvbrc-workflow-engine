#!/bin/bash
# Start the Workflow Executor (separate process)

echo "Starting Workflow Executor..."

cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "workflow_venv" ]; then
    echo "Activating virtual environment..."
    source workflow_venv/bin/activate
fi

# Start executor
echo "Starting executor process..."
python -m executor.main


