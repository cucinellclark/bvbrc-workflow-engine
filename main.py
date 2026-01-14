"""Main entry point for workflow engine."""
import sys
import argparse
from pathlib import Path

import uvicorn

from config.config import config
from utils.logger import setup_logger


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Workflow Engine Executor"
    )
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Path to configuration file (default: config/config.yaml)'
    )
    parser.add_argument(
        '--host',
        type=str,
        default=None,
        help='API host (overrides config)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=None,
        help='API port (overrides config)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default=None,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (overrides config)'
    )
    
    return parser.parse_args()


def main():
    """Main function to start the workflow engine."""
    args = parse_args()
    
    # Load configuration
    try:
        config.load(args.config)
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Setup logging
    log_level = args.log_level or config.get('logging.level', 'INFO')
    log_format = config.get(
        'logging.format',
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = setup_logger('workflow_engine', log_level, log_format)
    
    logger.info("=" * 60)
    logger.info("Workflow Engine Executor")
    logger.info("=" * 60)
    
    # Get API configuration
    host = args.host or config.get('api.host', '0.0.0.0')
    port = args.port or config.get('api.port', 8000)
    debug = config.get('api.debug', False)
    
    logger.info(f"API Configuration:")
    logger.info(f"  Host: {host}")
    logger.info(f"  Port: {port}")
    logger.info(f"  Debug: {debug}")
    
    # MongoDB configuration
    logger.info(f"MongoDB Configuration:")
    logger.info(f"  Host: {config.get('mongodb.host')}")
    logger.info(f"  Port: {config.get('mongodb.port')}")
    logger.info(f"  Database: {config.get('mongodb.database')}")
    logger.info(f"  Collection: {config.get('mongodb.collection')}")
    
    logger.info("=" * 60)
    
    # Start the API server
    try:
        logger.info("Starting API server...")
        uvicorn.run(
            "api.server:app",
            host=host,
            port=port,
            log_level=log_level.lower(),
            reload=debug
        )
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

