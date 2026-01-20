#!/bin/bash
# Start both API and Executor (development mode)

echo "========================================"
echo "Starting Workflow Engine (Development)"
echo "========================================"

cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "workflow_venv" ]; then
    echo "Activating virtual environment..."
    source workflow_venv/bin/activate
fi

# Start API in background
echo "Starting API server..."
./scripts/start_api.sh &
API_PID=$!
echo "API started with PID: $API_PID"

# Wait a bit for API to start
sleep 2

# Start Executor in background
echo "Starting Executor..."
./scripts/start_executor.sh &
EXECUTOR_PID=$!
echo "Executor started with PID: $EXECUTOR_PID"

echo ""
echo "========================================"
echo "Workflow Engine is running!"
echo "API: http://localhost:8000"
echo "Docs: http://localhost:8000/docs"
echo "Metrics: http://localhost:8000/metrics"
echo ""
echo "API PID: $API_PID"
echo "Executor PID: $EXECUTOR_PID"
echo ""
echo "Press Ctrl+C to stop both processes"
echo "========================================"

# Wait for user interrupt
trap "echo 'Stopping...'; kill $API_PID $EXECUTOR_PID 2>/dev/null; exit 0" INT TERM

# Keep script running
wait


