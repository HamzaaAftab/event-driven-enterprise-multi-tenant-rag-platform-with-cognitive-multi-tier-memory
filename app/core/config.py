"""
Application Configuration and Settings via Pydantic Settings.
"""

from typing import Literal, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # General App Settings
    ENVIRONMENT: str = "development"
    APP_NAME: str = "Enterprise Multi-Tenant RAG Platform"
    API_V1_STR: str = "/api/v1"
    DEBUG: bool = True

    # Supabase & PostgreSQL
    SUPABASE_URL: str = Field(default="", description="Supabase project API URL")
    SUPABASE_ANON_KEY: str = Field(default="", description="Supabase Anon Key")
    SUPABASE_SERVICE_ROLE_KEY: str = Field(default="", description="Supabase Service Role Key")
    SUPABASE_STORAGE_BUCKET: str = "documents"
    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/postgres",
        description="Async PostgreSQL connection string",
    )

    # Pinecone Vector DB (Serverless v5+)
    PINECONE_API_KEY: str = Field(default="", description="Pinecone API Key")
    PINECONE_INDEX_NAME: str = "enterprise-multitenant-rag"
    PINECONE_ENVIRONMENT: str = "us-east-1"
    EMBEDDING_DIMENSION: int = 768

    # Upstash Redis
    REDIS_URL: str = Field(
        default="redis://localhost:6379",
        description="Upstash Redis connection URL (rediss://...)",
    )

    # Aiven Kafka (Serverless SASL_SSL with CA Certificate)
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="", description="Kafka broker endpoints")
    KAFKA_SASL_USERNAME: str = Field(default="", description="Kafka SASL Username / Key")
    KAFKA_SASL_PASSWORD: str = Field(default="", description="Kafka SASL Password / Secret")
    KAFKA_SECURITY_PROTOCOL: str = "SASL_SSL"
    KAFKA_SASL_MECHANISM: str = "SCRAM-SHA-256"
    KAFKA_CA_CERT: str = Field(default="", description="Aiven Kafka CA PEM Certificate")
    KAFKA_TOPIC_INGESTION: str = "doc-ingestion-events"
    KAFKA_TOPIC_MEMORY: str = "memory-extraction-events"
    KAFKA_TOPIC_DLQ: str = "dead-letter-queue"

    # LlamaParse
    LLAMA_CLOUD_API_KEY: str = Field(default="", description="LlamaParse API Key")

    # Multi-Provider LLM Cascade (Groq -> NVIDIA -> OpenRouter -> Gemini)
    # 1. Primary: Groq
    GROQ_API_KEY: str = Field(default="", description="Groq Cloud API Key")
    GROQ_BASE_URL: str = "https://api.groq.com/openai/v1"
    GROQ_DEFAULT_MODEL: str = "llama-3.3-70b-versatile"

    # 2. Secondary: NVIDIA NIM
    NVIDIA_API_KEY: str = Field(default="", description="NVIDIA NIM API Key")
    NVIDIA_BASE_URL: str = "https://integrate.api.nvidia.com/v1"
    NVIDIA_DEFAULT_MODEL: str = "meta/llama-3.3-70b-instruct"

    # 3. Fallback: OpenRouter
    OPENROUTER_API_KEY: str = Field(default="", description="OpenRouter API Key")
    OPENROUTER_BASE_URL: str = "https://openrouter.ai/api/v1"
    OPENROUTER_DEFAULT_MODEL: str = "deepseek/deepseek-r1"

    # 4. Fallback: Google Gemini
    GEMINI_API_KEY: str = Field(default="", description="Google AI Studio Gemini API Key")
    GEMINI_BASE_URL: str = "https://generativelanguage.googleapis.com/v1beta/openai/"
    GEMINI_DEFAULT_MODEL: str = "gemini-2.0-flash"

    # Embeddings
    EMBEDDING_PROVIDER: str = "gemini"  # gemini | openrouter | local
    EMBEDDING_MODEL_NAME: str = "text-embedding-004"

    # Observability (LangSmith)
    LANGCHAIN_TRACING_V2: bool = True
    LANGCHAIN_API_KEY: str = Field(default="", description="LangSmith API Key")
    LANGCHAIN_PROJECT: str = "multitenant-rag-memory"
    LANGCHAIN_ENDPOINT: str = "https://api.smith.langchain.com"


settings = Settings()
