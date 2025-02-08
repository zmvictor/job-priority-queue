# High Availability Job Priority Queue

A single-node high availability job queue designed for managing long-running distributed training jobs (e.g., PyTorch distributed training) with dynamic priority adjustments and preemption support.

## Features

### High Availability on Single Node
- Persistent state using SQLite
- Automatic failover with leader election
- State reconciliation after node recovery
- No job loss during failover

### Long-Running Job Support
- Designed for distributed training workloads
- Preemption with clean job state handling
- Job status tracking through entire lifecycle
- Automatic job requeuing after preemption

### Job Metadata
Each job includes:
- `id`: Unique identifier
- `name`: Human-readable job name
- `timestamp`: Submission time (UTC)
- `status`: Current state (submit, pending, scheduled, running, etc.)
- `priority`: Base priority level
- `metadata`: Job-specific configuration
- `preemption_count`: Number of times preempted
- `wait_time_weight`: Dynamic priority boost factor

### Priority and Preemption
- Base priority determines initial job ordering
- Wait time increases priority within same band
  - Jobs waiting longer get higher priority
  - Priority boost based on wait time
  - Maximum 24x boost after 24 hours
- Preemption occurs when:
  - Higher priority job arrives
  - Lower priority job is running
  - Preempted jobs return to queue with wait time boost

## Development Setup
```bash
# Install dependencies
poetry install

# Run migrations
poetry run alembic upgrade head

# Start server
poetry run fastapi dev app/main.py
```

## Testing
```bash
# Run unit tests
poetry run pytest tests/

# Run integration tests
poetry run pytest tests/test_integration.py
```

## API Documentation
Once the server is running, visit http://localhost:8000/docs for the OpenAPI documentation.
