"""Workflow execution context for in-memory state tracking."""
from dataclasses import dataclass, field
from typing import Dict, Any, Set, List, Optional
from datetime import datetime
import logging
import networkx as nx

from core.dag_analyzer import DAGAnalyzer
from utils.workflow_logger import WorkflowLogger


@dataclass
class WorkflowExecutionContext:
    """In-memory context for workflow execution.
    
    This is built from MongoDB workflow documents and kept in memory
    for efficient execution tracking. It is NOT persisted - MongoDB
    is the source of truth.
    """
    
    workflow_id: str
    workflow_name: str
    status: str
    auth_token: Optional[str]
    
    # DAG representation
    dag: nx.DiGraph
    
    # Execution tracking sets (step_ids or step_names)
    completed_steps: Set[str] = field(default_factory=set)
    running_steps: Set[str] = field(default_factory=set)
    failed_steps: Set[str] = field(default_factory=set)
    
    # Configuration
    max_parallel_steps: int = 2
    
    # Logging
    workflow_logger: Optional[logging.Logger] = None
    
    # Metadata
    last_poll_time: Optional[datetime] = None
    total_steps: int = 0
    
    @classmethod
    def build_from_workflow_document(
        cls,
        workflow_doc: Dict[str, Any],
        log_dir: str = "logs/workflows"
    ) -> 'WorkflowExecutionContext':
        """Build execution context from MongoDB workflow document.
        
        Args:
            workflow_doc: Workflow document from MongoDB
            log_dir: Directory for workflow logs
            
        Returns:
            WorkflowExecutionContext instance
        """
        workflow_id = workflow_doc['workflow_id']
        workflow_name = workflow_doc['workflow_name']
        status = workflow_doc.get('status', 'pending')
        auth_token = workflow_doc.get('auth_token')
        
        # Build DAG
        dag = DAGAnalyzer.build_dag_from_workflow(workflow_doc)
        
        # Validate DAG
        DAGAnalyzer.validate_dag(dag)
        
        # Extract execution state from workflow
        steps = workflow_doc.get('steps', [])
        total_steps = len(steps)
        
        completed_steps = set()
        running_steps = set()
        failed_steps = set()
        
        for step in steps:
            # ALWAYS use step_name for consistency with DAG node IDs
            step_name = step.get('step_name')
            step_status = step.get('status', 'pending')
            
            if step_status == 'succeeded':
                completed_steps.add(step_name)
            elif step_status == 'running':
                running_steps.add(step_name)
            elif step_status == 'failed':
                failed_steps.add(step_name)
        
        # Get or create workflow logger
        workflow_logger = WorkflowLogger.get_logger(workflow_id, log_dir)
        
        # Get max parallel steps from execution metadata
        exec_meta = workflow_doc.get('execution_metadata', {})
        max_parallel = exec_meta.get('max_parallel_steps', 2)
        
        return cls(
            workflow_id=workflow_id,
            workflow_name=workflow_name,
            status=status,
            auth_token=auth_token,
            dag=dag,
            completed_steps=completed_steps,
            running_steps=running_steps,
            failed_steps=failed_steps,
            max_parallel_steps=max_parallel,
            workflow_logger=workflow_logger,
            last_poll_time=None,
            total_steps=total_steps
        )
    
    def can_submit_more_steps(self) -> bool:
        """Check if more steps can be submitted.
        
        Returns:
            True if under parallel execution limit
        """
        return len(self.running_steps) < self.max_parallel_steps
    
    def get_capacity(self) -> int:
        """Get number of additional steps that can be submitted.
        
        Returns:
            Number of available parallel slots
        """
        return max(0, self.max_parallel_steps - len(self.running_steps))
    
    def get_ready_steps(self) -> List[Dict[str, Any]]:
        """Get steps that are ready to run.
        
        Returns:
            List of step dictionaries ready for submission
        """
        return DAGAnalyzer.get_ready_steps(self.dag, self.completed_steps)
    
    def get_running_steps_list(self) -> List[Dict[str, Any]]:
        """Get list of currently running steps.
        
        Returns:
            List of running step dictionaries
        """
        return DAGAnalyzer.get_running_steps(self.dag)
    
    def is_complete(self) -> bool:
        """Check if workflow execution is complete.
        
        Returns:
            True if all steps are in terminal states
        """
        return DAGAnalyzer.is_workflow_complete(self.dag)
    
    def has_failed(self) -> bool:
        """Check if workflow has failed.
        
        Returns:
            True if any step has failed
        """
        return DAGAnalyzer.has_workflow_failed(self.dag)
    
    def has_succeeded(self) -> bool:
        """Check if workflow has succeeded.
        
        Returns:
            True if all steps succeeded
        """
        return DAGAnalyzer.has_workflow_succeeded(self.dag)
    
    def mark_step_completed(self, step_name: str) -> None:
        """Mark a step as completed.
        
        Args:
            step_name: Step name (used as identifier in DAG)
        """
        if step_name in self.running_steps:
            self.running_steps.remove(step_name)
        self.completed_steps.add(step_name)
    
    def mark_step_failed(self, step_name: str) -> None:
        """Mark a step as failed.
        
        Args:
            step_name: Step name (used as identifier in DAG)
        """
        if step_name in self.running_steps:
            self.running_steps.remove(step_name)
        self.failed_steps.add(step_name)
    
    def mark_step_running(self, step_name: str) -> None:
        """Mark a step as running.
        
        Args:
            step_name: Step name (used as identifier in DAG)
        """
        self.running_steps.add(step_name)
    
    def update_status(self, new_status: str) -> None:
        """Update workflow status.
        
        Args:
            new_status: New status value
        """
        self.status = new_status
    
    def refresh_dag_from_workflow(self, workflow_doc: Dict[str, Any]) -> None:
        """Refresh DAG with updated workflow data.
        
        This is called after MongoDB updates to sync in-memory state.
        
        Args:
            workflow_doc: Updated workflow document from MongoDB
        """
        # Rebuild DAG with updated step data
        self.dag = DAGAnalyzer.build_dag_from_workflow(workflow_doc)
        
        # Update status
        self.status = workflow_doc.get('status', self.status)

