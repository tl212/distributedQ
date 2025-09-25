#!/bin/bash

# distributedQ startup script

set -e

# colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # no color

echo -e "${GREEN}🚀 Starting DistributedQ${NC}"
echo "=================================="

# check if virtual environment exists
if [[ ! -d "venv" ]]; then
    echo -e "${YELLOW}📦 Creating virtual environment...${NC}"
    python -m venv venv
fi

# activate virtual environment
echo -e "${YELLOW}🔄 Activating virtual environment...${NC}"
source venv/bin/activate

# install dependencies
echo -e "${YELLOW}📚 Installing dependencies...${NC}"
pip install -r requirements.txt

# check for .env file
if [[ ! -f ".env" ]]; then
    echo -e "${YELLOW}⚙️  No .env file found, copying from .env.example${NC}"
    cp .env.example .env
    echo -e "${YELLOW}📝 Please edit .env file with your configuration${NC}"
fi

# load environment variables
if [[ -f ".env" ]]; then
    export $(cat .env | xargs)
fi

# check redis connection if enabled
if [[ "${USE_REDIS:-false}" == "true" ]]; then
    echo -e "${YELLOW}🔍 Checking Redis connection...${NC}"
    if python -c "import redis; redis.Redis.from_url('${REDIS_URL:-redis://localhost:6379/0}').ping()" 2>/dev/null; then
        echo -e "${GREEN}✅ Redis is running${NC}"
    else
        echo -e "${RED}❌ Redis is not running${NC}"
        echo "To start Redis:"
        echo "  docker-compose up -d redis"
        echo "  or"
        echo "  redis-server"
        exit 1
    fi
fi

# run tests
echo -e "${YELLOW}🧪 Running basic tests...${NC}"
python -m pytest distributed_queue/tests/test_queue.py::TestPriorityQueue::test_basic_put_and_get -v

echo -e "${GREEN}✅ Tests passed!${NC}"

# start the api
echo -e "${GREEN}🎯 Starting DistributedQ API server...${NC}"
echo "API will be available at: http://localhost:${API_PORT:-8000}"
echo "API Documentation: http://localhost:${API_PORT:-8000}/docs"
echo "Health Check: http://localhost:${API_PORT:-8000}/api/v1/health"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

python run_api.py