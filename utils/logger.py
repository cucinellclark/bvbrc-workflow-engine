"""Logging configuration for workflow engine."""
import logging
import sys
from pathlib import Path
from typing import Optional
from logging.handlers import RotatingFileHandler


_loggers = {}


def setup_logger(
    name: str = "workflow_engine",
    level: str = "INFO",
    log_format: Optional[str] = None,
    log_file: Optional[str] = None,
    console_level: Optional[str] = None
) -> logging.Logger:
    """Set up and configure logger with both console and file handlers.
    
    Args:
        name: Logger name
        level: Logging level for file handler (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string
        log_file: Optional log file path. If provided, adds rotating file handler
        console_level: Optional separate logging level for console (defaults to level)
        
    Returns:
        Configured logger instance
    """
    if name in _loggers:
        return _loggers[name]
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Set to DEBUG, handlers will filter
    
    # Remove existing handlers
    logger.handlers = []
    
    # Format
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    formatter = logging.Formatter(log_format)
    
    # Console handler with potentially different level
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, (console_level or level).upper()))
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log_file specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Rotating file handler: 10MB per file, keep 5 backups
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
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

