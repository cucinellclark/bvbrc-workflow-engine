"""Workflow executor main entry point.

Run this as a separate process from the API server:
    python -m executor.main
"""
import sys
import asyncio
import signal
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.state_manager import StateManager
from scheduler.client import SchedulerClient
from executor.workflow_executor import WorkflowExecutor
from config.config import config
from utils.logger import get_logger


logger = get_logger(__name__)

# Global executor instance
executor: WorkflowExecutor = None


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    if executor:
        asyncio.create_task(executor.stop())
    sys.exit(0)


async def main():
    """Main executor loop."""
    global executor
    
    try:
        logger.info("=" * 60)
        logger.info("Starting Workflow Executor")
        logger.info("=" * 60)
        
        # Initialize dependencies
        logger.info("Initializing components...")
        
        # Log configuration
        mongo_config = config.mongodb
        logger.info(f"MongoDB configuration: {mongo_config.get('host')}:{mongo_config.get('port')}/{mongo_config.get('database')}")
        
        # State manager (MongoDB)
        state_manager = StateManager()
        logger.info("✓ StateManager initialized")
        
        # Scheduler client
        scheduler_config = config.scheduler
        scheduler_client = SchedulerClient(
            scheduler_url=scheduler_config.get('url'),
            timeout=scheduler_config.get('timeout', 30)
        )
        logger.info("✓ SchedulerClient initialized")
        
        # Workflow executor
        executor = WorkflowExecutor(
            state_manager=state_manager,
            scheduler_client=scheduler_client,
            config=config
        )
        logger.info("✓ WorkflowExecutor initialized")
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        logger.info("✓ Signal handlers registered")
        
        # Start executor
        await executor.start()
        
        logger.info("=" * 60)
        logger.info("Workflow Executor is running")
        logger.info(f"Polling interval: {executor.polling_interval}s")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 60)
        
        # Keep running
        while True:
            await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    
    except Exception as e:
        logger.error(f"Fatal error in executor: {e}", exc_info=True)
        sys.exit(1)
    
    finally:
        if executor:
            logger.info("Shutting down executor...")
            await executor.stop()
            logger.info("Executor shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Executor stopped by user")
    except Exception as e:
        logger.error(f"Executor failed: {e}", exc_info=True)
        sys.exit(1)

