#!/bin/bash
# Stop API and Executor processes

echo "Stopping Workflow Engine..."

# Find and kill uvicorn (API)
echo "Stopping API server..."
pkill -f "uvicorn api.server:app"

# Find and kill executor
echo "Stopping Executor..."
pkill -f "python -m executor.main"

echo "Workflow Engine stopped."


