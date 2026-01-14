#!/bin/bash

# Workflow Engine Executor - Start Script
# This script starts the workflow engine API server

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Change to the script directory
cd "$SCRIPT_DIR"

# Activate virtual environment if it exists
if [ -d "workflow_venv" ]; then
    echo "Activating virtual environment..."
    source workflow_venv/bin/activate
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 is not installed or not in PATH"
    exit 1
fi

# Check if requirements are installed
echo "Checking dependencies..."
python3 -c "import fastapi" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "Warning: Required packages may not be installed."
    echo "Install them with: pip install -r requirements.txt"
    echo ""
fi

# Set default configuration file
CONFIG_FILE="${SCRIPT_DIR}/config/config.yaml"

# Parse command line arguments
HOST=""
PORT=""
LOG_LEVEL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --config PATH      Path to configuration file (default: config/config.yaml)"
            echo "  --host HOST        API host (overrides config)"
            echo "  --port PORT        API port (overrides config)"
            echo "  --log-level LEVEL  Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL"
            echo "  -h, --help         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Build command
CMD="python3 main.py"

if [ ! -z "$CONFIG_FILE" ]; then
    CMD="$CMD --config $CONFIG_FILE"
fi

if [ ! -z "$HOST" ]; then
    CMD="$CMD --host $HOST"
fi

if [ ! -z "$PORT" ]; then
    CMD="$CMD --port $PORT"
fi

if [ ! -z "$LOG_LEVEL" ]; then
    CMD="$CMD --log-level $LOG_LEVEL"
fi

echo "Starting Workflow Engine..."
echo "Command: $CMD"
echo ""

# Execute the command
exec $CMD

