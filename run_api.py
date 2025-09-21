#!/usr/bin/env python
"""
Run the Distributed Queue API server
"""

import uvicorn
from distributed_queue.api.app import app

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=True
    )