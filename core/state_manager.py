"""MongoDB state management for workflows."""
from typing import Optional, Dict, Any, List
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from datetime import datetime

from config.config import config
from utils.logger import get_logger
from models.workflow import WorkflowSubmission


logger = get_logger(__name__)


class StateManager:
    """Manages workflow state in MongoDB."""
    
    def __init__(self):
        """Initialize MongoDB connection."""
        self.client: Optional[MongoClient] = None
        self.db = None
        self.collection = None
        self._connect()
    
    def _connect(self):
        """Establish connection to MongoDB."""
        mongo_config = config.mongodb
        
        try:
            # Build connection string
            host = mongo_config.get('host', 'localhost')
            port = mongo_config.get('port', 27017)
            username = mongo_config.get('username')
            password = mongo_config.get('password')
            auth_source = mongo_config.get('auth_source', 'admin')
            
            if username and password:
                connection_string = (
                    f"mongodb://{username}:{password}@{host}:{port}/"
                    f"?authSource={auth_source}"
                )
            else:
                connection_string = f"mongodb://{host}:{port}/"
            
            logger.info(f"Connecting to MongoDB at {host}:{port}")
            self.client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=5000
            )
            
            # Test connection
            self.client.admin.command('ping')
            
            # Get database and collection
            db_name = mongo_config.get('database', 'workflow_engine_db')
            collection_name = mongo_config.get('collection', 'workflows')
            
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            
            # Create index on workflow_id for fast lookups
            self.collection.create_index(
                [("workflow_id", ASCENDING)],
                unique=True
            )
            
            logger.info(
                f"Connected to MongoDB: {db_name}.{collection_name}"
            )
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        except Exception as e:
            logger.error(f"Error initializing MongoDB: {e}")
            raise
    
    def save_workflow(self, workflow_data: Dict[str, Any]) -> str:
        """Save workflow to MongoDB.
        
        Args:
            workflow_data: Workflow data dictionary
            
        Returns:
            workflow_id of saved workflow
            
        Raises:
            DuplicateKeyError: If workflow_id already exists
            Exception: For other database errors
        """
        try:
            workflow_id = workflow_data.get('workflow_id')
            logger.info(f"Saving workflow {workflow_id} to MongoDB")
            
            # Ensure timestamps
            if 'created_at' not in workflow_data:
                workflow_data['created_at'] = datetime.utcnow()
            if 'updated_at' not in workflow_data:
                workflow_data['updated_at'] = datetime.utcnow()
            
            # Insert document
            result = self.collection.insert_one(workflow_data)
            
            logger.info(
                f"Workflow {workflow_id} saved with _id: {result.inserted_id}"
            )
            return workflow_id
            
        except DuplicateKeyError:
            logger.error(f"Workflow {workflow_id} already exists")
            raise ValueError(f"Workflow {workflow_id} already exists")
        except Exception as e:
            logger.error(f"Error saving workflow: {e}")
            raise
    
    def get_workflow(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve workflow by ID.
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            Workflow document or None if not found
        """
        try:
            logger.debug(f"Retrieving workflow {workflow_id}")
            workflow = self.collection.find_one(
                {"workflow_id": workflow_id},
                {"_id": 0}  # Exclude MongoDB _id from result
            )
            
            if workflow:
                logger.debug(f"Found workflow {workflow_id}")
            else:
                logger.debug(f"Workflow {workflow_id} not found")
            
            return workflow
            
        except Exception as e:
            logger.error(f"Error retrieving workflow {workflow_id}: {e}")
            raise
    
    def update_workflow_status(
        self, 
        workflow_id: str, 
        status: str
    ) -> bool:
        """Update workflow status.
        
        Args:
            workflow_id: Workflow identifier
            status: New status value
            
        Returns:
            True if updated, False if not found
        """
        try:
            logger.info(
                f"Updating workflow {workflow_id} status to {status}"
            )
            
            result = self.collection.update_one(
                {"workflow_id": workflow_id},
                {
                    "$set": {
                        "status": status,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.matched_count == 0:
                logger.warning(f"Workflow {workflow_id} not found for update")
                return False
            
            logger.info(f"Workflow {workflow_id} status updated")
            return True
            
        except Exception as e:
            logger.error(
                f"Error updating workflow {workflow_id} status: {e}"
            )
            raise
    
    def update_step_status(
        self,
        workflow_id: str,
        step_id: str,
        status: str
    ) -> bool:
        """Update status of a specific step.
        
        Args:
            workflow_id: Workflow identifier
            step_id: Step identifier
            status: New status value
            
        Returns:
            True if updated, False if not found
        """
        try:
            logger.info(
                f"Updating step {step_id} in workflow {workflow_id} "
                f"status to {status}"
            )
            
            result = self.collection.update_one(
                {
                    "workflow_id": workflow_id,
                    "steps.step_id": step_id
                },
                {
                    "$set": {
                        "steps.$.status": status,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            
            if result.matched_count == 0:
                logger.warning(
                    f"Step {step_id} in workflow {workflow_id} not found"
                )
                return False
            
            logger.info(f"Step {step_id} status updated")
            return True
            
        except Exception as e:
            logger.error(f"Error updating step {step_id} status: {e}")
            raise
    
    def list_workflows(
        self,
        limit: int = 100,
        skip: int = 0,
        status_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List workflows with optional filtering.
        
        Args:
            limit: Maximum number of results
            skip: Number of results to skip
            status_filter: Optional status to filter by
            
        Returns:
            List of workflow documents
        """
        try:
            query = {}
            if status_filter:
                query['status'] = status_filter
            
            workflows = list(
                self.collection.find(
                    query,
                    {"_id": 0}
                )
                .sort("created_at", -1)
                .skip(skip)
                .limit(limit)
            )
            
            logger.debug(f"Retrieved {len(workflows)} workflows")
            return workflows
            
        except Exception as e:
            logger.error(f"Error listing workflows: {e}")
            raise
    
    def close(self):
        """Close MongoDB connection."""
        if self.client:
            logger.info("Closing MongoDB connection")
            self.client.close()

