"""Logging configuration for workflow engine."""
import logging
import sys
from typing import Optional


_loggers = {}


def setup_logger(name: str = "workflow_engine", level: str = "INFO", 
                log_format: Optional[str] = None) -> logging.Logger:
    """Set up and configure logger.
    
    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string
        
    Returns:
        Configured logger instance
    """
    if name in _loggers:
        return _loggers[name]
    
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    logger.handlers = []
    
    # Console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, level.upper()))
    
    # Format
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    _loggers[name] = logger
    return logger


def get_logger(name: str = "workflow_engine") -> logging.Logger:
    """Get existing logger or create new one.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    if name in _loggers:
        return _loggers[name]
    return setup_logger(name)

