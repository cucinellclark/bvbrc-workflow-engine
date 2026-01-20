# Workflow Engine - DAG-Based Execution System

A production-ready workflow execution engine with DAG (Directed Acyclic Graph) support for bioinformatics pipelines.

## Features

- **DAG-Based Execution**: Workflows are executed as directed acyclic graphs with automatic dependency resolution
- **Parallel Execution**: Up to 2 steps per workflow can run in parallel (configurable)
- **State Persistence**: All workflow state stored in MongoDB for reliability and resumability
- **Resume Capability**: Executor automatically resumes workflows after restart
- **Per-Workflow Logging**: Structured logs for each workflow execution
- **Metrics & Observability**: Prometheus metrics endpoint for monitoring
- **REST API**: Full REST API for workflow submission, status queries, and cancellation
- **Scheduler Integration**: Integrates with BV-BRC scheduler via JSON-RPC

## Architecture

The system consists of two separate processes:

1. **API Server** (`api/server.py`): Stateless FastAPI server that handles HTTP requests
2. **Executor** (`executor/main.py`): Background process that orchestrates workflow execution

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ HTTP
       ▼
┌─────────────────────────────────┐
│       API Server (Port 8000)    │
│  - Submit workflows             │
│  - Query status                 │
│  - Cancel workflows             │
│  - Prometheus metrics           │
└──────┬──────────────────────────┘
       │
       ▼
┌─────────────────────────────────┐
│         MongoDB                 │
│  - Workflow documents           │
│  - Step status & metadata       │
│  - Auth tokens                  │
└──────┬──────────────────────────┘
       │
       ▼
┌─────────────────────────────────┐
│  Executor (Separate Process)    │
│  - Poll every 10s               │
│  - Build DAG with NetworkX      │
│  - Submit steps to scheduler    │
│  - Check job status             │
│  - Update MongoDB               │
│  - Write logs                   │
└──────┬──────────────────────────┘
       │
       ▼
┌─────────────────────────────────┐
│  BV-BRC Scheduler (JSON-RPC)    │
│  - Execute jobs                 │
│  - Return task status           │
└─────────────────────────────────┘
```

## Installation

### 1. Install Dependencies

```bash
cd /home/ac.cucinell/bvbrc-dev/WorkflowEngineDev/workflow_engine

# Activate virtual environment
source workflow_venv/bin/activate

# Install required packages
pip install -r requirements.txt
```

### 2. Configure

Edit `config/config.yaml`:

```yaml
mongodb:
  host: 140.221.78.16
  port: 27018
  database: copilot
  collection: workflows

scheduler:
  url: https://p3.theseed.org/services/app_service
  timeout: 30

executor:
  polling_interval_seconds: 10        # How often to poll (adjustable)
  max_parallel_steps_per_workflow: 2  # Max parallel steps per workflow
  enable_auto_resume: true             # Resume workflows on restart

logging:
  level: INFO
  workflow_log_dir: "logs/workflows"
  executor_log_file: "logs/executor.log"

metrics:
  enable_prometheus: true
  port: 9090
```

## Usage

### Starting the System

**Option 1: Start All (Development)**
```bash
./scripts/start_all.sh
```

**Option 2: Start Separately**
```bash
# Terminal 1: Start API
./scripts/start_api.sh

# Terminal 2: Start Executor
./scripts/start_executor.sh
```

### Stopping the System

```bash
./scripts/stop_all.sh
```

### Submit a Workflow

```bash
curl -X POST http://localhost:8000/api/v1/workflows/submit \
  -H "Content-Type: application/json" \
  -H "Authorization: your_auth_token" \
  -d @example_workflow_2step.json
```

Response:
```json
{
  "workflow_id": "wf_1737320000_1234",
  "status": "pending",
  "message": "Workflow submitted successfully"
}
```

### Check Workflow Status

```bash
curl http://localhost:8000/api/v1/workflows/wf_1737320000_1234/status
```

Response:
```json
{
  "workflow_id": "wf_1737320000_1234",
  "workflow_name": "assembly-to-annotation-pipeline",
  "status": "running",
  "created_at": "2026-01-19T10:00:00Z",
  "updated_at": "2026-01-19T10:05:23Z",
  "steps": [
    {
      "step_id": "19542145",
      "step_name": "assemble",
      "status": "succeeded",
      "app": "GenomeAssembly2"
    },
    {
      "step_id": "19542148",
      "step_name": "annotate",
      "status": "running",
      "app": "GenomeAnnotation"
    }
  ]
}
```

### Cancel a Workflow

```bash
curl -X POST http://localhost:8000/api/v1/workflows/wf_1737320000_1234/cancel
```

### View Prometheus Metrics

```bash
curl http://localhost:8000/metrics
```

## Workflow Status Lifecycle

```
pending → queued → running → succeeded/failed/cancelled
```

**Status Definitions:**
- `pending`: Submitted, waiting for executor to pick up
- `queued`: Executor picked it up, analyzing dependencies
- `running`: At least one step is executing
- `succeeded`: All steps completed successfully
- `failed`: One or more steps failed
- `cancelled`: User cancelled the workflow

## Step Status Lifecycle

```
pending → ready → queued → running → succeeded/failed
```

**Status Definitions:**
- `pending`: Waiting for dependencies
- `ready`: Dependencies met, ready to submit
- `queued`: Submitted to scheduler, waiting
- `running`: Currently executing
- `succeeded`: Completed successfully
- `failed`: Execution failed
- `upstream_failed`: Skipped because dependency failed

## Workflow JSON Format

```json
{
  "workflow_name": "my-pipeline",
  "version": "1.0",
  "base_context": {
    "base_url": "https://www.bv-brc.org",
    "workspace_output_folder": "/user/home/output"
  },
  "steps": [
    {
      "step_name": "step1",
      "app": "GenomeAssembly2",
      "params": {
        "output_path": "${workspace_output_folder}/Assembly",
        "output_file": "assembly_output"
      },
      "outputs": {
        "contigs": "${params.output_path}/.${params.output_file}/contigs.fa"
      },
      "depends_on": []
    },
    {
      "step_name": "step2",
      "app": "GenomeAnnotation",
      "params": {
        "contigs": "${steps.step1.outputs.contigs}",
        "output_path": "${workspace_output_folder}/Annotation"
      },
      "outputs": {},
      "depends_on": ["step1"]
    }
  ],
  "workflow_outputs": [
    "${steps.step2.outputs.genome}"
  ]
}
```

## Logs

### Per-Workflow Logs

Each workflow gets its own log file:
```
logs/workflows/wf_1737320000_1234.log
```

Example log entry:
```
2026-01-19 10:00:00 - INFO - Workflow execution started
2026-01-19 10:00:01 - INFO - Submitted step 'assemble' to app 'GenomeAssembly2' | task_id=19542145
2026-01-19 10:04:33 - INFO - Step 'assemble' completed successfully | elapsed_time=00:04:32
2026-01-19 10:04:35 - INFO - Submitted step 'annotate' to app 'GenomeAnnotation' | task_id=19542148
```

### Executor Logs

Main executor log:
```
logs/executor.log
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/workflows/submit` | Submit a new workflow |
| GET | `/api/v1/workflows/{id}/status` | Get workflow status |
| GET | `/api/v1/workflows/{id}` | Get full workflow document |
| POST | `/api/v1/workflows/{id}/cancel` | Cancel a workflow |
| POST | `/api/v1/workflows/submit-cwl` | Submit CWL workflow |
| GET | `/api/v1/health` | Health check |
| GET | `/metrics` | Prometheus metrics |
| GET | `/docs` | Interactive API documentation |

## Monitoring

### Prometheus Metrics

Key metrics exposed at `/metrics`:

**Workflow Metrics:**
- `workflows_submitted_total`: Total workflows submitted
- `workflows_completed_total{status}`: Total completed (by status)
- `active_workflows_count`: Currently active workflows
- `workflow_execution_duration_seconds`: Workflow durations

**Step Metrics:**
- `steps_submitted_total{app}`: Total steps submitted (by app)
- `steps_completed_total{app,status}`: Total completed (by app and status)
- `step_execution_duration_seconds{app}`: Step durations (by app)

**Executor Metrics:**
- `executor_poll_cycles_total`: Total poll cycles
- `executor_poll_duration_seconds`: Poll cycle durations
- `executor_errors_total{error_type}`: Executor errors

**Scheduler Metrics:**
- `scheduler_query_duration_seconds`: Scheduler query times
- `scheduler_query_errors_total`: Query errors
- `scheduler_submit_errors_total{app}`: Submission errors

## Configuration

### Polling Interval

Adjust how often the executor checks for updates:

```yaml
executor:
  polling_interval_seconds: 10  # Default: 10 seconds
```

Lower values = more responsive, higher MongoDB/scheduler load  
Higher values = less responsive, lower resource usage

### Parallel Execution Limit

Control how many steps can run simultaneously per workflow:

```yaml
executor:
  max_parallel_steps_per_workflow: 2  # Default: 2
```

### Auto-Resume

Enable/disable automatic workflow resumption on executor restart:

```yaml
executor:
  enable_auto_resume: true  # Default: true
```

## Development

### Project Structure

```
workflow_engine/
├── api/                    # FastAPI server
│   ├── server.py          # Main app
│   └── routes.py          # API endpoints
├── executor/              # Workflow executor
│   ├── main.py           # Entry point
│   ├── workflow_executor.py  # Main logic
│   └── workflow_context.py   # Execution context
├── core/                  # Core components
│   ├── dag_analyzer.py   # DAG analysis (NetworkX)
│   ├── workflow_manager.py
│   ├── state_manager.py  # MongoDB operations
│   └── validator.py
├── scheduler/             # Scheduler integration
│   └── client.py         # JSON-RPC client
├── utils/                 # Utilities
│   ├── workflow_logger.py  # Per-workflow logging
│   ├── metrics.py        # Prometheus metrics
│   └── jsonrpc_client.py
├── models/                # Pydantic models
│   └── workflow.py
├── config/                # Configuration
│   ├── config.yaml
│   └── config.py
├── scripts/               # Startup scripts
│   ├── start_api.sh
│   ├── start_executor.sh
│   ├── start_all.sh
│   └── stop_all.sh
└── logs/                  # Log files
    ├── workflows/         # Per-workflow logs
    └── executor.log       # Main executor log
```

### Key Technologies

- **FastAPI**: REST API framework
- **NetworkX**: DAG analysis and graph algorithms
- **APScheduler**: Robust job scheduling for executor
- **structlog**: Structured logging
- **Prometheus**: Metrics and monitoring
- **MongoDB**: State persistence
- **Pydantic**: Data validation

## Troubleshooting

### Executor Not Processing Workflows

1. Check if executor is running:
   ```bash
   ps aux | grep "python -m executor.main"
   ```

2. Check executor logs:
   ```bash
   tail -f logs/executor.log
   ```

3. Verify MongoDB connection:
   ```bash
   curl http://localhost:8000/api/v1/health
   ```

### Workflow Stuck in 'pending'

- Executor might not be running
- Check executor logs for errors
- Verify workflow was saved to MongoDB

### Step Not Submitting

- Check auth token is valid
- Verify scheduler URL in config
- Check scheduler client logs
- Verify step parameters are valid

### Workflow Not Resuming After Restart

- Ensure `enable_auto_resume: true` in config
- Check workflow status in MongoDB (should be 'running' or 'queued')
- Review executor startup logs

## Security Notes

⚠️ **Current Implementation**: Auth tokens are stored in plaintext in MongoDB

**TODO**: Implement token encryption before production deployment

Recommended approach:
1. Use environment variable for encryption key
2. Encrypt tokens before storing
3. Decrypt when needed for scheduler calls

## Production Deployment

### Systemd Services

**API Service** (`/etc/systemd/system/workflow-api.service`):
```ini
[Unit]
Description=Workflow Engine API
After=network.target mongodb.service

[Service]
Type=simple
User=workflow
WorkingDirectory=/path/to/workflow_engine
ExecStart=/path/to/workflow_venv/bin/uvicorn api.server:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```

**Executor Service** (`/etc/systemd/system/workflow-executor.service`):
```ini
[Unit]
Description=Workflow Executor
After=network.target mongodb.service
Wants=workflow-api.service

[Service]
Type=simple
User=workflow
WorkingDirectory=/path/to/workflow_engine
ExecStart=/path/to/workflow_venv/bin/python -m executor.main
Restart=always

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable workflow-api workflow-executor
sudo systemctl start workflow-api workflow-executor
```

## Support

For issues, questions, or contributions, contact the development team.

## License

[Add your license information here]
