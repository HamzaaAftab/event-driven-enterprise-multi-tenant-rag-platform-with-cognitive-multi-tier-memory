"""
FastAPI Main Application Entrypoint.
Enterprise Multi-Tenant RAG Platform with Cognitive Multi-Tier Memory.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import api_router
from app.core.config import settings
from app.core.kafka_producer import kafka_producer
from app.core.redis_client import redis_service

logging.basicConfig(
    level=logging.INFO if not settings.DEBUG else logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup and graceful shutdown."""
    logger.info("[STARTUP] Initializing Enterprise Multi-Tenant RAG Backend...")
    # Initialize Kafka Producer connection pool
    try:
        await kafka_producer.start()
    except Exception as e:
        logger.warning("[STARTUP WARN] Kafka producer failed to connect on startup: %s", e)

    yield

    logger.info("[SHUTDOWN] Closing connections...")
    await kafka_producer.stop()
    await redis_service.close()
    logger.info("[SHUTDOWN] Backend successfully closed.")


app = FastAPI(
    title=settings.APP_NAME,
    description="Production-grade Enterprise Multi-Tenant RAG Platform with 4-Tier Cognitive Memory Engine.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API v1 routes
app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/health", tags=["System Health"])
async def health_check():
    """Health check endpoint to verify backend service status."""
    return {
        "status": "healthy",
        "app_name": settings.APP_NAME,
        "environment": settings.ENVIRONMENT,
    }


@app.get("/", tags=["System Root"])
async def root():
    """Root landing endpoint."""
    return {
        "message": f"Welcome to {settings.APP_NAME}",
        "docs": "/docs",
        "health": "/health",
        "api_v1": settings.API_V1_STR,
    }
