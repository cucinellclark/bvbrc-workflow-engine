"""Prometheus metrics for workflow execution monitoring."""
from prometheus_client import Counter, Gauge, Histogram


# Workflow metrics
workflows_submitted_total = Counter(
    'workflows_submitted_total',
    'Total number of workflows submitted'
)

workflows_completed_total = Counter(
    'workflows_completed_total',
    'Total number of workflows completed',
    ['status']  # Labels: succeeded, failed, cancelled
)

active_workflows_count = Gauge(
    'active_workflows_count',
    'Number of currently active workflows'
)

pending_workflows_count = Gauge(
    'pending_workflows_count',
    'Number of workflows waiting to start'
)

workflow_execution_duration_seconds = Histogram(
    'workflow_execution_duration_seconds',
    'Workflow execution time in seconds',
    buckets=[60, 300, 600, 1800, 3600, 7200, 14400, 28800]  # 1m to 8h
)


# Step metrics
steps_submitted_total = Counter(
    'steps_submitted_total',
    'Total number of steps submitted to scheduler',
    ['app']  # Label: application name
)

steps_completed_total = Counter(
    'steps_completed_total',
    'Total number of steps completed',
    ['app', 'status']  # Labels: app name, succeeded/failed
)

active_steps_count = Gauge(
    'active_steps_count',
    'Number of currently running steps across all workflows'
)

step_execution_duration_seconds = Histogram(
    'step_execution_duration_seconds',
    'Step execution time in seconds',
    ['app'],  # Label: application name
    buckets=[30, 60, 120, 300, 600, 1800, 3600, 7200]  # 30s to 2h
)


# Scheduler interaction metrics
scheduler_query_duration_seconds = Histogram(
    'scheduler_query_duration_seconds',
    'Time taken to query scheduler status',
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30]  # 100ms to 30s
)

scheduler_query_errors_total = Counter(
    'scheduler_query_errors_total',
    'Total number of scheduler query errors'
)

scheduler_submit_errors_total = Counter(
    'scheduler_submit_errors_total',
    'Total number of scheduler submission errors',
    ['app']
)


# Executor metrics
executor_poll_cycles_total = Counter(
    'executor_poll_cycles_total',
    'Total number of executor poll cycles completed'
)

executor_poll_duration_seconds = Histogram(
    'executor_poll_duration_seconds',
    'Time taken for each executor poll cycle',
    buckets=[0.5, 1, 2, 5, 10, 30, 60]
)

executor_errors_total = Counter(
    'executor_errors_total',
    'Total number of executor errors',
    ['error_type']
)


def record_workflow_submitted() -> None:
    """Record a workflow submission."""
    workflows_submitted_total.inc()


def record_workflow_completed(status: str) -> None:
    """Record a workflow completion.
    
    Args:
        status: Final workflow status (succeeded, failed, cancelled)
    """
    workflows_completed_total.labels(status=status).inc()


def record_step_submitted(app: str) -> None:
    """Record a step submission.
    
    Args:
        app: Application name
    """
    steps_submitted_total.labels(app=app).inc()


def record_step_completed(app: str, status: str) -> None:
    """Record a step completion.
    
    Args:
        app: Application name
        status: Step status (succeeded, failed)
    """
    steps_completed_total.labels(app=app, status=status).inc()


def record_step_duration(app: str, duration_seconds: float) -> None:
    """Record step execution duration.
    
    Args:
        app: Application name
        duration_seconds: Execution time in seconds
    """
    step_execution_duration_seconds.labels(app=app).observe(duration_seconds)


def record_workflow_duration(duration_seconds: float) -> None:
    """Record workflow execution duration.
    
    Args:
        duration_seconds: Execution time in seconds
    """
    workflow_execution_duration_seconds.observe(duration_seconds)


def update_active_workflows(count: int) -> None:
    """Update active workflows gauge.
    
    Args:
        count: Number of active workflows
    """
    active_workflows_count.set(count)


def update_pending_workflows(count: int) -> None:
    """Update pending workflows gauge.
    
    Args:
        count: Number of pending workflows
    """
    pending_workflows_count.set(count)


def update_active_steps(count: int) -> None:
    """Update active steps gauge.
    
    Args:
        count: Number of active steps across all workflows
    """
    active_steps_count.set(count)


def record_scheduler_query_duration(duration_seconds: float) -> None:
    """Record scheduler query duration.
    
    Args:
        duration_seconds: Query time in seconds
    """
    scheduler_query_duration_seconds.observe(duration_seconds)


def record_scheduler_query_error() -> None:
    """Record a scheduler query error."""
    scheduler_query_errors_total.inc()


def record_scheduler_submit_error(app: str) -> None:
    """Record a scheduler submission error.
    
    Args:
        app: Application name
    """
    scheduler_submit_errors_total.labels(app=app).inc()


def record_poll_cycle() -> None:
    """Record completion of an executor poll cycle."""
    executor_poll_cycles_total.inc()


def record_poll_duration(duration_seconds: float) -> None:
    """Record executor poll cycle duration.
    
    Args:
        duration_seconds: Poll cycle time in seconds
    """
    executor_poll_duration_seconds.observe(duration_seconds)


def record_executor_error(error_type: str) -> None:
    """Record an executor error.
    
    Args:
        error_type: Type/category of error
    """
    executor_errors_total.labels(error_type=error_type).inc()

