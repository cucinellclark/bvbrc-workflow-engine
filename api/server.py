"""FastAPI server configuration."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from api.routes import router, set_workflow_manager
from core.workflow_manager import WorkflowManager
from utils.logger import get_logger


logger = get_logger(__name__)

# Global workflow manager
workflow_manager_instance: WorkflowManager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup
    global workflow_manager_instance
    logger.info("Starting up workflow engine API")
    
    try:
        workflow_manager_instance = WorkflowManager()
        set_workflow_manager(workflow_manager_instance)
        logger.info("Workflow engine initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize workflow engine: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down workflow engine API")
    if workflow_manager_instance:
        workflow_manager_instance.close()


# Create FastAPI application
app = FastAPI(
    title="Workflow Engine API",
    description="API for submitting and managing bioinformatics workflows",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Workflow Engine API",
        "version": "1.0.0",
        "docs": "/docs"
    }

