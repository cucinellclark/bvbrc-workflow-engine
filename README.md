# Workflow Engine Executor

A Python-based workflow engine for managing and executing bioinformatics workflows with MongoDB state management and REST API interface.

## Features

- ✅ JSON workflow validation with Pydantic
- ✅ MongoDB state management for workflow tracking
- ✅ Scheduler integration (placeholder implementation)
- ✅ REST API with FastAPI
- ✅ Dependency graph validation
- ✅ Variable reference validation
- ✅ Comprehensive logging

## Architecture

```
workflow_engine/
├── api/              # FastAPI server and routes
├── config/           # Configuration management
├── core/             # Core business logic
│   ├── validator.py        # JSON validation
│   ├── state_manager.py    # MongoDB operations
│   └── workflow_manager.py # Workflow orchestration
├── models/           # Pydantic data models
├── scheduler/        # Scheduler client (placeholders)
├── utils/            # Logging and utilities
├── config/config.yaml      # Configuration file
├── main.py           # Entry point
└── start_server.sh   # Server startup script
```

## Installation

### Prerequisites

- Python 3.8 or higher
- MongoDB (running locally or accessible remotely)

### Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure MongoDB:**
   Edit `config/config.yaml` to match your MongoDB setup:
   ```yaml
   mongodb:
     host: localhost
     port: 27017
     database: workflow_engine_db
     collection: workflows
   ```

3. **Environment Variables (Optional):**
   Override configuration with environment variables:
   ```bash
   export MONGODB_HOST=your_host
   export MONGODB_PORT=27017
   export MONGODB_USERNAME=your_username
   export MONGODB_PASSWORD=your_password
   ```

## Usage

### Starting the Server

**Using the start script:**
```bash
./start_server.sh
```

**With custom options:**
```bash
./start_server.sh --host 0.0.0.0 --port 8080 --log-level DEBUG
```

**Using Python directly:**
```bash
python3 main.py --config config/config.yaml --port 8000
```

The API will be available at `http://localhost:8000`

### API Documentation

Interactive API documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### API Endpoints

#### 1. Submit Workflow
**POST** `/api/v1/workflows/submit`

Submit a new workflow for execution.

**Request Body:**
```json
{
  "workflow_name": "assembly-to-annotation-pipeline",
  "version": "1.0",
  "base_context": {
    "base_url": "https://www.bv-brc.org",
    "workspace_output_folder": "/clark.cucinell@patricbrc.org/home/WorkspaceOutputFolder"
  },
  "steps": [
    {
      "step_name": "assemble",
      "app": "Assembly2",
      "params": {
        "paired_end_libs": [{"read1": "/.../reads_R1.fq", "read2": "/.../reads_R2.fq"}],
        "recipe": "auto",
        "output_path": "${workspace_root}/Assembly",
        "output_file": "my_assembly"
      },
      "outputs": {
        "contigs_fasta": "${params.output_path}/${params.output_file}.contigs.fasta"
      }
    },
    {
      "step_name": "annotate",
      "app": "Annotation",
      "depends_on": ["assemble"],
      "params": {
        "contigs": "${steps.assemble.outputs.contigs_fasta}",
        "scientific_name": "Streptococcus pneumoniae",
        "taxonomy_id": 1313,
        "output_path": "${workspace_root}/Annotation",
        "output_file": "my_annotation"
      },
      "outputs": {
        "genome_object": "${params.output_path}/.${params.output_file}/genome.gto"
      }
    }
  ],
  "workflow_outputs": [
    "${steps[1].outputs['genome_object']}"
  ]
}
```

**Response (201 Created):**
```json
{
  "workflow_id": "wf_1705234567890_1234",
  "status": "submitted",
  "message": "Workflow submitted successfully"
}
```

#### 2. Get Workflow Status
**GET** `/api/v1/workflows/{workflow_id}/status`

Get the current status of a workflow.

**Response (200 OK):**
```json
{
  "workflow_id": "wf_1705234567890_1234",
  "workflow_name": "assembly-to-annotation-pipeline",
  "status": "submitted",
  "created_at": "2024-01-14T12:00:00",
  "updated_at": "2024-01-14T12:00:00",
  "steps": [
    {
      "step_id": "step_1705234567890_0_5678",
      "step_name": "assemble",
      "status": "pending",
      "app": "Assembly2"
    },
    {
      "step_id": "step_1705234567890_1_5679",
      "step_name": "annotate",
      "status": "pending",
      "app": "Annotation"
    }
  ]
}
```

#### 3. Get Full Workflow
**GET** `/api/v1/workflows/{workflow_id}`

Get the complete workflow document.

#### 4. Health Check
**GET** `/api/v1/health`

Check the health status of the service.

**Response:**
```json
{
  "status": "healthy",
  "mongodb": "connected",
  "version": "1.0.0"
}
```

## Workflow JSON Format

### Input Format (Submission)
- Must NOT contain `workflow_id` or `step_id` fields
- These are assigned by the scheduler during submission

### Stored Format (MongoDB)
- Contains `workflow_id` and `step_id` fields
- Includes status tracking
- Contains timestamps

### Validation Rules

1. **Required Fields:**
   - `workflow_name`, `version`, `base_context`, `steps`
   - Each step: `step_name`, `app`, `params`

2. **Dependencies:**
   - `depends_on` must reference valid step names
   - No circular dependencies allowed

3. **Variable References:**
   - Must use `${...}` syntax
   - Step references must point to existing steps

## Configuration

### config.yaml

```yaml
mongodb:
  host: localhost          # MongoDB host
  port: 27017             # MongoDB port
  database: workflow_engine_db
  collection: workflows
  username: null          # Optional authentication
  password: null
  auth_source: admin

api:
  host: 0.0.0.0          # API server host
  port: 8000             # API server port
  debug: false           # Debug mode

scheduler:
  url: http://localhost:9000  # Scheduler URL (placeholder)
  timeout: 30

logging:
  level: INFO            # DEBUG, INFO, WARNING, ERROR, CRITICAL
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

## MongoDB Schema

Workflows are stored with the following structure:

```json
{
  "_id": ObjectId("..."),
  "workflow_id": "wf_1705234567890_1234",
  "workflow_name": "assembly-to-annotation-pipeline",
  "version": "1.0",
  "status": "submitted",
  "created_at": ISODate("2024-01-14T12:00:00Z"),
  "updated_at": ISODate("2024-01-14T12:00:00Z"),
  "base_context": {...},
  "steps": [
    {
      "step_id": "step_...",
      "step_name": "assemble",
      "app": "Assembly2",
      "status": "pending",
      "params": {...},
      "outputs": {...},
      "depends_on": []
    }
  ],
  "workflow_outputs": [...]
}
```

**Indexes:**
- `workflow_id`: Unique index for fast lookups

## Scheduler Integration

The current implementation uses placeholder functions for scheduler interaction:

- `submit_workflow_to_scheduler()`: Generates mock workflow and step IDs
- `get_scheduler_status()`: Returns mock execution status
- `cancel_workflow()`: Logs cancellation request

These placeholders are located in `scheduler/client.py` and should be replaced with actual scheduler API calls.

## Logging

Logs include:
- Workflow submission events
- Validation results
- Database operations
- API requests and responses
- Error details

Log level can be configured in `config.yaml` or via command line:
```bash
./start_server.sh --log-level DEBUG
```

## Error Handling

### HTTP Status Codes
- `200 OK`: Successful retrieval
- `201 Created`: Successful submission
- `400 Bad Request`: Validation errors
- `404 Not Found`: Workflow not found
- `500 Internal Server Error`: Server errors

### Validation Errors
- Missing required fields
- Invalid step dependencies
- Circular dependencies
- Invalid variable references
- Presence of ID fields in input

## Development

### Project Status
This is a minimal skeleton implementation with:
- ✅ Complete validation logic
- ✅ MongoDB state management
- ✅ REST API interface
- ✅ Placeholder scheduler integration

### Future Enhancements
- Replace placeholder scheduler functions with real API calls
- Add workflow execution monitoring
- Implement step-level status updates from scheduler
- Add authentication/authorization
- Add workflow cancellation endpoint
- Add pagination for workflow listing
- Add workflow search/filtering capabilities

## Troubleshooting

### MongoDB Connection Issues
- Ensure MongoDB is running: `systemctl status mongod`
- Check connection settings in `config.yaml`
- Verify network connectivity

### Import Errors
- Ensure all dependencies are installed: `pip install -r requirements.txt`
- Check Python version: `python3 --version` (requires 3.8+)

### Port Already in Use
- Change port in config or via command line: `--port 8080`
- Check for existing process: `lsof -i :8000`

## License

Internal use for BV-BRC workflow management.

