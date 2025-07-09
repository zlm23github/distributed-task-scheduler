# Distributed Task Scheduler

A robust distributed task processing system built with Python, RabbitMQ, and Redis. This system supports multiple workers with load balancing, monitoring, and fault tolerance.

## 🏗️ Architecture Overview

The system consists of several modular components designed for scalability and maintainability:

### Core Components

- **API Server** (`api_server/`) - RESTful API for task submission and management
- **Task Workers** (`worker/`) - Distributed workers for task processing
- **Client** (`client/`) - Task generation and submission utilities
- **Message Queue** (RabbitMQ) - Task distribution and load balancing
- **State Storage** (Redis) - Task status tracking and worker registry

## 📊 System Architecture

```mermaid
graph TB
    A[Client] --> B[API Server]
    B --> C[RabbitMQ Queue]
    C --> D[Worker 1]
    C --> E[Worker 2]
    C --> F[Worker N]
    D --> G[Redis]
    E --> G
    F --> G
    B --> G
    
    subgraph "Task Flow"
        H[Task Submission] --> I[Task Queue]
        I --> J[Worker Processing]
        J --> K[Status Update]
        K --> L[Result Storage]
    end
    
    subgraph "Worker Management"
        M[Worker Registration] --> N[Heartbeat Monitoring]
        N --> O[Health Check]
        O --> P[Auto Recovery]
    end
```

## 🧩 Module Structure

```
distributed-task-scheduler/
├── api_server/                 # REST API server
│   ├── app/
│   │   ├── main.py            # FastAPI application
│   │   ├── models.py          # Pydantic models
│   │   ├── services.py        # Business logic
│   │   └── dependencies.py    # Dependency injection
│   └── requirements.txt       # API dependencies
├── worker/                     # Worker components
│   ├── distributed_worker.py  # Main worker coordinator
│   ├── task_worker.py         # Task processing logic
│   ├── connection_manager.py  # Connection management
│   ├── worker_factory.py      # Worker creation factory
│   ├── worker_registry.py     # Worker registration
│   ├── lifecycle_manager.py   # Worker lifecycle
│   └── task_handlers.py       # Task type handlers
├── client/                     # Client utilities
│   └── task_generator.py      # Task generation
├── tests/                      # Test suite
├── start_workers.sh           # Worker startup script
├── stop_workers.sh            # Worker shutdown script
├── check_workers.sh           # Worker status checker
└── docker-compose.yml         # Infrastructure setup
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Docker & Docker Compose
- RabbitMQ
- Redis

### 1. Start Infrastructure

```bash
# Start RabbitMQ and Redis
docker-compose up -d
```

### 2. Start API Server

```bash
cd api_server
pip install -r requirements.txt
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 3. Start Workers

```bash
# Start 3 workers (default)
./start_workers.sh

# Start 5 workers
./start_workers.sh 5

# Start with custom prefix
./start_workers.sh 3 my-worker
```

### 4. Submit Tasks

```bash
# Generate and submit tasks
python client/task_generator.py
```

## 🛠️ Management Commands

### Worker Management

```bash
# Start workers
./start_workers.sh [num_workers]

# Check worker status
./check_workers.sh

# Stop all workers
./stop_workers.sh
```

### Monitoring

```bash
# Check running processes
ps aux | grep distributed_worker

# Check Redis worker registry
redis-cli KEYS "worker:*"

```

## 🧪 Testing

### Run All Tests

```bash
# Test the complete system
python tests/test_api.py
python tests/test_client.py
python tests/test_connection.py
python tests/test_distributed_worker.py

```

## 📋 API Endpoints

### Task Management

- `POST /tasks/` - Submit new task
- `GET /tasks/{task_id}` - Get task status
- `GET /tasks/` - List all tasks
- `DELETE /tasks/{task_id}` - Delete task

### Worker Management

- `GET /workers/` - List all workers
- `GET /workers/{worker_id}` - Get worker status
- `DELETE /workers/{worker_id}` - Remove worker

## 🔧 Configuration

### Environment Variables

```bash
# RabbitMQ
RABBITMQ_URL=amqp://admin:admin123@localhost:5672/

# Redis
REDIS_URL=redis://localhost:6379/0

# API Server
API_HOST=0.0.0.0
API_PORT=8000
```

### Worker Configuration

```bash
# Worker settings
WORKER_ID=worker-1
LOG_LEVEL=INFO
HEARTBEAT_INTERVAL=30
```

## 🏗️ Architecture Details

### Task Processing Flow

1. **Task Submission** - Client submits task via API
2. **Queue Distribution** - Task sent to RabbitMQ queue
3. **Worker Consumption** - Available worker picks up task
4. **Task Processing** - Worker executes task logic
5. **Status Update** - Progress and results stored in Redis
6. **Completion** - Final result returned to client

### Worker Lifecycle

1. **Registration** - Worker registers with Redis
2. **Heartbeat** - Periodic health updates
3. **Task Processing** - Consume and process tasks
4. **Graceful Shutdown** - Cleanup and deregistration

### Fault Tolerance

- **Worker Failover** - Automatic task redistribution
- **Heartbeat Monitoring** - Dead worker detection
- **Task Retry** - Failed task recovery
- **Connection Resilience** - Automatic reconnection

## 📊 Monitoring

### Health Checks

```bash
# Check API health
curl http://localhost:8000/health

# Check worker status
./check_workers.sh

# Monitor Redis
redis-cli INFO
```

### Metrics

- Task completion rate
- Worker utilization
- Queue depth
- Error rates
- Response times



## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Add tests
4. Submit pull request

## 📄 License

MIT License - see LICENSE file for details

## 🆘 Support

For issues and questions, please check the documentation or create an issue.

---

**Built with ❤️ using Python, RabbitMQ, and Redis**