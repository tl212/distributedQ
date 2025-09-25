# DistributedQ Quick Start Guide

## 🚀 Getting Started

### Option 1: Quick Start Script (Recommended)
```bash
./start.sh
```

This script will:
- Create a virtual environment
- Install dependencies
- Copy configuration from .env.example
- Run basic tests
- Start the API server

### Option 2: Manual Setup
```bash
# create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# install dependencies
pip install -r requirements.txt

# copy configuration
cp .env.example .env

# start API server
python run_api.py
```

## 🎯 Testing the API

Once the server is running:

### Health Check
```bash
curl http://localhost:8000/api/v1/health
```

### Submit a Task
```bash
curl -X POST "http://localhost:8000/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "priority": 1,
    "payload": {
      "type": "simple",
      "data": "Hello DistributedQ!"
    }
  }'
```

### View Queue Stats
```bash
curl http://localhost:8000/api/v1/stats
```

### API Documentation
Visit `http://localhost:8000/docs` for interactive API documentation.

## 🐳 Docker Deployment

### Development
```bash
docker-compose up
```

### Production
```bash
# set environment variables
export API_KEY=your-secret-key
export REDIS_PASSWORD=your-redis-password

# start with production config
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## 📊 Monitoring

### Terminal Dashboard
```bash
python distributed_queue/monitoring/dashboard.py
```

### Prometheus Metrics
Visit `http://localhost:8000/api/v1/metrics` for Prometheus metrics.

## 🧪 Running Tests

```bash
# run all tests
python -m pytest distributed_queue/tests/ -v

# test specific functionality
python test_api.py
python test_redis_backend.py  # requires Redis
```

## 📋 Examples

### Basic Usage
```bash
python distributed_queue/examples/basic_usage.py
```

### Distributed Demo (Redis)
```bash
# start Redis first
docker-compose up -d redis

# run distributed demo
python distributed_queue/examples/distributed_demo.py
```

## ⚙️ Configuration

Key environment variables in `.env`:

- `USE_REDIS=true` - Enable Redis backend
- `API_KEY=your-key` - Secure write endpoints
- `NUM_WORKERS=3` - Number of worker threads
- `MAX_QUEUE_SIZE=10000` - Queue capacity limit
- `LOG_LEVEL=INFO` - Logging verbosity

## 🔧 Development

### Project Structure
```
distributedQ/
├── distributed_queue/
│   ├── api/          # FastAPI application
│   ├── core/         # Queue and worker logic
│   ├── monitoring/   # Metrics and dashboard
│   ├── storage/      # Redis backend
│   └── tests/        # Test suite
├── examples/         # Usage examples
├── docker-compose.yml
└── requirements.txt
```

### Adding New Task Types
1. Register handler in worker:
```python
def my_handler(data):
    # process data
    return result

worker.register_handler("my_type", my_handler)
```

2. Submit tasks with that type:
```python
task = Task(
    priority=1,
    payload={"type": "my_type", "data": {...}}
)
```

## 🆘 Troubleshooting

### Redis Connection Issues
```bash
# check if Redis is running
redis-cli ping

# start Redis with Docker
docker-compose up -d redis
```

### Port Already in Use
```bash
# check what's using port 8000
lsof -i :8000

# or change port in .env
API_PORT=8080
```

### Import Errors
```bash
# ensure you're in the right directory and venv is activated
pwd  # should show .../distributedQ
which python  # should show venv/bin/python
```

## 📚 Next Steps

- Read the full [README.md](README.md) for detailed documentation
- Explore [examples/](examples/) for more use cases
- Check [tests/](distributed_queue/tests/) for implementation details
- Review [monitoring/](distributed_queue/monitoring/) for observability features