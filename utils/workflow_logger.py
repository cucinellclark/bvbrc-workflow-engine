"""Per-workflow logging utilities with structured logging support."""
import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import structlog
from pathlib import Path


# Configure structlog for workflow logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)


class WorkflowLogger:
    """Manages per-workflow log files with structured logging."""
    
    _loggers: Dict[str, logging.Logger] = {}
    
    @classmethod
    def get_logger(
        cls,
        workflow_id: str,
        log_dir: str = "logs/workflows"
    ) -> logging.Logger:
        """Get or create a logger for a specific workflow.
        
        Args:
            workflow_id: Workflow identifier
            log_dir: Directory to store workflow logs
            
        Returns:
            Logger instance configured for this workflow
        """
        if workflow_id in cls._loggers:
            return cls._loggers[workflow_id]
        
        # Create log directory if it doesn't exist
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        
        # Create log file path
        log_file = log_path / f"{workflow_id}.log"
        
        # Create logger
        logger = logging.getLogger(f"workflow.{workflow_id}")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False  # Don't propagate to root logger
        
        # Remove existing handlers
        logger.handlers.clear()
        
        # Create file handler
        file_handler = logging.FileHandler(str(log_file))
        file_handler.setLevel(logging.DEBUG)
        
        # Create formatter (simple format for workflow logs)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        
        # Cache the logger
        cls._loggers[workflow_id] = logger
        
        logger.info(f"Workflow logger initialized: {workflow_id}")
        
        return logger
    
    @classmethod
    def get_structured_logger(cls, workflow_id: str) -> structlog.BoundLogger:
        """Get structured logger bound to workflow context.
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            Structlog BoundLogger with workflow_id in context
        """
        # Get base logger
        base_logger = cls.get_logger(workflow_id)
        
        # Wrap with structlog and bind workflow_id
        struct_logger = structlog.wrap_logger(base_logger)
        return struct_logger.bind(workflow_id=workflow_id)
    
    @staticmethod
    def log_workflow_event(
        logger: logging.Logger,
        event: str,
        level: str = "INFO",
        **kwargs
    ) -> None:
        """Log a workflow event with additional context.
        
        Args:
            logger: Logger instance
            event: Event name/description
            level: Log level (DEBUG, INFO, WARNING, ERROR)
            **kwargs: Additional context key-value pairs
        """
        # Build message
        context_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        message = f"{event}"
        if context_str:
            message += f" | {context_str}"
        
        # Log at appropriate level
        log_level = getattr(logging, level.upper(), logging.INFO)
        logger.log(log_level, message)
    
    @staticmethod
    def log_step_transition(
        logger: logging.Logger,
        step_name: str,
        old_status: str,
        new_status: str,
        **kwargs
    ) -> None:
        """Log a step status transition.
        
        Args:
            logger: Logger instance
            step_name: Name of the step
            old_status: Previous status
            new_status: New status
            **kwargs: Additional context (task_id, error, etc.)
        """
        context = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        message = f"Step '{step_name}' transition: {old_status} → {new_status}"
        if context:
            message += f" | {context}"
        
        logger.info(message)
    
    @staticmethod
    def log_step_submission(
        logger: logging.Logger,
        step_name: str,
        app: str,
        task_id: str
    ) -> None:
        """Log step submission to scheduler.
        
        Args:
            logger: Logger instance
            step_name: Name of the step
            app: Application name
            task_id: Scheduler task ID
        """
        logger.info(
            f"Submitted step '{step_name}' to app '{app}' | task_id={task_id}"
        )
    
    @staticmethod
    def log_step_completion(
        logger: logging.Logger,
        step_name: str,
        elapsed_time: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log step completion.
        
        Args:
            logger: Logger instance
            step_name: Name of the step
            elapsed_time: Execution time
            **kwargs: Additional context
        """
        context = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        message = f"Step '{step_name}' completed successfully"
        if elapsed_time:
            message += f" | elapsed_time={elapsed_time}"
        if context:
            message += f" | {context}"
        
        logger.info(message)
    
    @staticmethod
    def log_step_failure(
        logger: logging.Logger,
        step_name: str,
        error_message: str,
        **kwargs
    ) -> None:
        """Log step failure.
        
        Args:
            logger: Logger instance
            step_name: Name of the step
            error_message: Error description
            **kwargs: Additional context
        """
        context = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        message = f"Step '{step_name}' FAILED | error={error_message}"
        if context:
            message += f" | {context}"
        
        logger.error(message)
    
    @staticmethod
    def log_workflow_start(
        logger: logging.Logger,
        workflow_name: str,
        total_steps: int
    ) -> None:
        """Log workflow execution start.
        
        Args:
            logger: Logger instance
            workflow_name: Name of the workflow
            total_steps: Total number of steps
        """
        logger.info(
            f"=== Workflow Execution Started: {workflow_name} ==="
        )
        logger.info(f"Total steps: {total_steps}")
    
    @staticmethod
    def log_workflow_completion(
        logger: logging.Logger,
        workflow_name: str,
        status: str,
        duration: Optional[str] = None
    ) -> None:
        """Log workflow completion.
        
        Args:
            logger: Logger instance
            workflow_name: Name of the workflow
            status: Final status (succeeded/failed)
            duration: Total execution time
        """
        logger.info(
            f"=== Workflow Execution Completed: {workflow_name} ==="
        )
        logger.info(f"Final status: {status}")
        if duration:
            logger.info(f"Total duration: {duration}")
    
    @classmethod
    def close_logger(cls, workflow_id: str) -> None:
        """Close and remove a workflow logger.
        
        Args:
            workflow_id: Workflow identifier
        """
        if workflow_id in cls._loggers:
            logger = cls._loggers[workflow_id]
            
            # Close all handlers
            for handler in logger.handlers:
                handler.close()
            
            # Remove logger
            del cls._loggers[workflow_id]

