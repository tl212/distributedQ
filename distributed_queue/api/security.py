"""
Simple API key authentication for write endpoints.
Set environment variable API_KEY to enable protection. If API_KEY is unset,
all endpoints remain open (useful for local development).
"""
from fastapi import Security, HTTPException, status
from fastapi.security.api_key import APIKeyHeader
import os

API_KEY_HEADER_NAME = "X-API-Key"
_api_key_header = APIKeyHeader(name=API_KEY_HEADER_NAME, auto_error=False)


def require_api_key(api_key: str = Security(_api_key_header)) -> None:
    """
    Dependency to protect endpoints with an API key.
    - if API_KEY env var is not set, do nothing (open mode)
    - if set, require matching X-API-Key header
    """
    required = os.getenv("API_KEY")
    if not required:
        return None  # Open mode
    if api_key and api_key == required:
        return None
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key",
        headers={"WWW-Authenticate": "API key"},
    )
