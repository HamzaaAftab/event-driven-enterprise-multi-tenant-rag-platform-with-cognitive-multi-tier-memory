# Enterprise Multi-Tenant RAG Platform with Cognitive Multi-Tier Memory
 
> An enterprise-grade, event-driven, production-ready Multi-Tenant Retrieval-Augmented Generation (RAG) platform with strict tenant data isolation, 4-tier cognitive memory architecture, layout-preserving document ingestion, serverless messaging, and multi-provider LLM failover.

---

## 📌 Table of Contents
- [Executive Overview](#-executive-overview)
- [Key Features](#-key-features)
- [System Architecture](#-system-architecture)
- [4-Tier Cognitive Memory Architecture](#-4-tier-cognitive-memory-architecture)
- [Multi-Provider LLM Fallback Cascade](#-multi-provider-llm-fallback-cascade)
- [Technology Stack](#-technology-stack)
- [Repository Structure](#-repository-structure)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation & Setup](#installation--setup)
  - [Environment Configuration](#environment-configuration)
  - [Running the Application](#running-the-application)
- [API Endpoints](#-api-endpoints)
- [Documentation Index](#-documentation-index)

---

## 🚀 Executive Overview

Traditional RAG architectures suffer from tenant data leakage risks, slow sync document parsing, single-provider LLM outages, and lack of persistent user memory across sessions.

This platform solves these challenges through:
1. **Strict Multi-Tenant Isolation:** Complete data separation at every layer (Supabase Row-Level Security, dedicated Pinecone namespaces per tenant, and tenant-scoped storage buckets).
2. **Event-Driven Asynchronous Pipeline:** File uploads return immediately (`202 Accepted`) while Upstash Kafka distributes heavy PDF parsing (LlamaParse), chunking, and embedding across background workers.
3. **Cognitive Multi-Tier Memory:** Emulates human cognitive faculties across 4 distinct storage and retrieval tiers (Working, Factual, Episodic, and Procedural).
4. **Resilient LLM Cascades:** Automatic zero-downtime failover across four OpenAI-compatible providers (**Groq ➔ NVIDIA NIM ➔ OpenRouter ➔ Google Gemini**) tracked end-to-end via **LangSmith**.

---

## 🌟 Key Features

- 🛡️ **Zero-Leakage Multi-Tenancy:** Hard partition isolation via `tenant_{tenant_id}_docs` and `tenant_{tenant_id}_memory` in Pinecone, alongside PostgreSQL RLS policies.
- 📄 **Layout & Table-Aware PDF Parsing:** Integration with **LlamaParse** preserving financial tables, multi-column layouts, and markdown structures without splitting tabular data.
- 🧠 **Human-Like Cognitive Memory:**
  - *Working Memory:* Sub-millisecond context caching with Upstash Redis sliding window.
  - *Factual Memory:* Profile and entity extraction stored in PostgreSQL `user_facts`.
  - *Episodic Memory:* Long-term conversation episode summarization indexed in Pinecone.
  - *Procedural Memory:* Tenant-specific business logic, rules, and system behavior.
- ⚡ **Serverless Cloud Messaging:** Upstash Kafka (SASL_SSL) event streams with automatic retry loops and Dead-Letter Queues (DLQ).
- 🔄 **Universal Multi-LLM Provider Engine:** Transparent cascade across Groq (LLaMA 3.3 70B), NVIDIA NIM, OpenRouter (DeepSeek R1), and Gemini 2.0 Flash.
- 📊 **Enterprise Observability:** Complete tracing of latency, token usage, cost analytics, and fallback transitions using LangSmith.

---

## 🏗️ System Architecture

```
+----------------------------------------------------------------------------------------------------+
|                                    1. CLIENT & ACCESS LAYER                                        |
|  +--------------------------------------------+    +--------------------------------------------+  |
|  |       Tenant User Interface                |    |       Tenant Admin Dashboard               |  |
|  |  (Chat, PDF Upload, Citation Viewer)       |    |  (User Management, Usage Quota, Analytics) |  |
|  +--------------------------------------------+    +--------------------------------------------+  |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|                            2. API GATEWAY & SECURITY LAYER (FastAPI)                               |
|  +--------------------------+  +-------------------------------+  +-----------------------------+  |
|  | Supabase Auth / JWT      |  | Tenant Context Resolution     |  | Rate Limiting & Metering    |  |
|  | (Token & Role Validator) |  | (tenant_id, user_id, role)    |  | (Upstash Redis Rate Limiter)|  |
|  +--------------------------+  +-------------------------------+  +-----------------------------+  |
+----------------------------------------------------------------------------------------------------+
           |                                              |                             |
           v (File Uploads)                               v (Events)                    v (Queries)
+------------------------+             +----------------------------------+    +---------------------+
| 3. OBJECT STORAGE      |             | 4. SERVERLESS EVENT STREAM       |    | 5. RAG ORCHESTRATOR |
| Supabase Storage       |             | (Upstash Kafka / Cloud Kafka)    |    | (Query Rewriter,    |
| Path:                  |             |                                  |    |  Multi-Retriever,   |
| /tenants/{id}/docs/    |             | Topic: doc-ingestion-events      |    |  Context Synthesis, |
|                        |             | Topic: memory-extraction-events  |    |  SSE Stream Output) |
+------------------------+             +----------------------------------+    +---------------------+
           |                                              |                             |
           | (Reads binary)                               v (Consumes events)           | (Retrieves context)
           |                           +----------------------------------+             |
           +-------------------------->| 6. ASYNC DISTRIBUTED WORKERS     |             |
                                       |                                  |             |
                                       | Ingestion Worker:                |             |
                                       | - LlamaParse (Markdown + Tables) |             |
                                       | - Hierarchical Semantic Chunker  |             |
                                       | - Embeddings Engine              |             |
                                       |                                  |             |
                                       | Memory Worker:                   |             |
                                       | - Fact Extraction LLM            |             |
                                       | - Episodic Summarizer LLM        |             |
                                       +----------------------------------+             |
                                              |                    |                    |
                                              v                    v                    v
+----------------------------------------------------------------------------------------------------+
|                              7. MULTI-MODEL STORAGE & ISOLATION LAYER                              |
|                                                                                                    |
|  +--------------------------------+  +--------------------------------+  +----------------------+  |
|  | Supabase PostgreSQL           |  | Pinecone Serverless Vector DB  |  | Upstash Redis Cache  |  |
|  | - Tenants & Users (RBAC)       |  | - Docs: tenant_{id}_docs       |  | - Working Memory     |  |
|  | - user_facts (Factual Memory)  |  | - Memory: tenant_{id}_memory   |  |   (Sliding Window)   |  |
|  | - Chat Sessions & Messages     |  | - Strict Metadata Filters      |  | - Tenant Policy      |  |
|  | - Documents & Chunks Metadata  |  |                                |  |   Cache              |  |
|  +--------------------------------+  +--------------------------------+  +----------------------+  |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|                              8. ENTERPRISE OBSERVABILITY (LangSmith)                               |
|  - Full-Trace LLM Chains & RAG Latency Breakdown                                                   |
|  - Token Consumption & Multi-Tenant Cost Analytics                                                 |
|  - Real-Time Fallback Monitoring (Groq ➔ NVIDIA ➔ OpenRouter ➔ Gemini)                             |
+----------------------------------------------------------------------------------------------------+
```

---

## 🧠 4-Tier Cognitive Memory Architecture

| Memory Tier | Cognitive Function | Storage Engine | Retrieval Method | Lifecycle / TTL |
| :--- | :--- | :--- | :--- | :--- |
| **1. Working Memory** | Immediate conversational focus & turn-by-turn context | **Upstash Redis Cache** | Sliding window fetch (last $N$ messages / 2,000 tokens) | 24-Hour Sliding TTL |
| **2. Factual Memory** | Explicit user facts, preferences, domain entities, traits | **Supabase PostgreSQL** (`user_facts`) | Deterministic SQL key-value & entity filter | Permanent (User/Admin editable) |
| **3. Episodic Memory** | Past conversation summaries & historical insights | **Pinecone Serverless** (`tenant_{id}_memory`) | Semantic vector search filtered by user & tenant | Long-term indexed vector store |
| **4. Procedural Memory** | Tenant business guidelines, system instructions, guardrails | **Supabase PostgreSQL** (`tenants.policy_rules`) | Injected during prompt synthesis | Admin controlled per tenant |

---

## 🔀 Multi-Provider LLM Fallback Cascade

To ensure 99.99% availability, the platform orchestrates an automated failover sequence across OpenAI-compatible providers:

```mermaid
graph LR
    A[Client Request] --> B[1. Groq Cloud<br/>llama-3.3-70b-versatile]
    B -- On RateLimit / Error --> C[2. NVIDIA NIM<br/>llama-3.3-70b-instruct]
    C -- On RateLimit / Error --> D[3. OpenRouter<br/>deepseek-r1]
    D -- On RateLimit / Error --> E[4. Google Gemini<br/>gemini-2.0-flash]
    B & C & D & E --> F[LangSmith Tracing]
```

---

## 🛠️ Technology Stack

| Layer | Technologies & Services |
| :--- | :--- |
| **Web & API Framework** | [FastAPI](https://fastapi.tiangolo.com/), [Uvicorn](https://www.uvicorn.org/), [Pydantic v2](https://docs.pydantic.dev/) |
| **Database & Auth** | [Supabase](https://supabase.com/) (PostgreSQL 15+, Supabase Auth, Row-Level Security) |
| **Object Storage** | Supabase Storage (Private S3-compatible tenant buckets) |
| **Vector Database** | [Pinecone Serverless v5+](https://www.pinecone.io/) (Namespace-isolated) |
| **Document Parser** | [LlamaParse](https://cloud.llamaindex.ai/) (Markdown & Table Preservation) |
| **Event Streaming** | [Upstash Kafka](https://upstash.com/docs/kafka) (Serverless SASL_SSL SCRAM-SHA-256) |
| **Caching & Working Memory** | [Upstash Redis](https://upstash.com/docs/redis) |
| **LLM & Embeddings** | Groq, NVIDIA NIM, OpenRouter, Google Gemini (`text-embedding-004`) |
| **Observability** | [LangSmith](https://smith.langchain.com/) |

---

## 📁 Repository Structure

```
multitenancy-rag-architecture/
├── .env.example              # Template for all environment variables
├── requirements.txt          # Python production dependencies
├── README.md                 # Project documentation & overview
│
├── app/                      # Application source code
│   ├── __init__.py
│   ├── api/                  # API endpoints & routing
│   │   ├── __init__.py
│   │   └── v1/               # Version 1 API routers
│   ├── core/                 # Core configuration & infrastructure
│   │   ├── __init__.py
│   │   └── config.py         # Pydantic Settings management
│   ├── db/                   # Database clients & session management
│   ├── memory/               # 4-tier cognitive memory handlers
│   ├── rag/                  # RAG orchestration, retrieval & synthesis
│   ├── schemas/              # Pydantic models & event schemas
│   ├── services/             # LLM, Vector, Parser & Cache services
│   └── workers/              # Async background workers (Kafka consumers)
│
├── docs/                     # Detailed architectural specifications
│   ├── overview.md           # System overview & high-level architecture
│   ├── architecture_and_flow.md # Deep dive into sequence & data flows
│   ├── database_schema.md    # Complete PostgreSQL DDL & RLS specifications
│   ├── implementation_plan.md# Detailed technical implementation plan
│   └── steps_tasks.md        # Step-by-step roadmap & task checklist
│
├── scripts/                  # Helper scripts & database migrations
└── tests/                    # Unit, integration & multi-tenant isolation tests
```

---

## ⚙️ Getting Started

### Prerequisites
- **Python:** 3.11 or higher
- **Cloud Accounts & API Keys:**
  - [Supabase](https://supabase.com/) (Project URL, Anon Key, Service Role Key, Database URL)
  - [Pinecone](https://www.pinecone.io/) (API Key, Index Name)
  - [Upstash](https://upstash.com/) (Kafka Bootstrap Server, Credentials, Redis URL)
  - [LlamaCloud](https://cloud.llamaindex.ai/) (LlamaParse API Key)
  - [Groq](https://console.groq.com/) / [Google AI Studio](https://aistudio.google.com/) / [NVIDIA](https://build.nvidia.com/) / [OpenRouter](https://openrouter.ai/)
  - [LangSmith](https://smith.langchain.com/) (API Key for tracing)

### Installation & Setup

1. **Clone the Repository:**
   ```bash
   git clone <repository-url>
   cd multitenancy-rag-architecture
   ```

2. **Create and Activate Virtual Environment:**
   ```bash
   python -m venv .venv
   # Windows:
   .venv\Scripts\activate
   # Linux / macOS:
   source .venv/bin/activate
   ```

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### Environment Configuration

Copy `.env.example` to `.env` and fill in your service credentials:

```bash
cp .env.example .env
```

Key environment variables in `.env`:
```ini
# Application
ENVIRONMENT="development"
APP_NAME="Enterprise Multi-Tenant RAG Platform"

# Supabase
SUPABASE_URL="https://your-project.supabase.co"
SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
DATABASE_URL="postgresql+asyncpg://postgres:password@db.your-project.supabase.co:5432/postgres"

# Pinecone
PINECONE_API_KEY="your-pinecone-api-key"
PINECONE_INDEX_NAME="enterprise-multitenant-rag"

# Upstash Kafka & Redis
REDIS_URL="rediss://default:password@your-redis.upstash.io:6379"
KAFKA_BOOTSTRAP_SERVERS="your-kafka.upstash.io:9092"
KAFKA_SASL_USERNAME="your-kafka-username"
KAFKA_SASL_PASSWORD="your-kafka-password"

# Parser & LLMs
LLAMA_CLOUD_API_KEY="your-llamaparse-api-key"
GROQ_API_KEY="your-groq-api-key"
GEMINI_API_KEY="your-gemini-api-key"

# Observability
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY="your-langsmith-key"
LANGCHAIN_PROJECT="multitenant-rag-memory"
```

### Running the Application

1. **Start the FastAPI Server:**
   ```bash
   uvicorn app.api.v1:app --host 0.0.0.0 --port 8000 --reload
   ```

2. **Access Swagger Interactive Documentation:**
   - Swagger UI: `http://localhost:8000/docs`
   - ReDoc: `http://localhost:8000/redoc`

---

## 📡 API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/v1/documents/upload` | Upload PDF; stores file in Supabase Storage and pushes event to Upstash Kafka. |
| `POST` | `/api/v1/chat/stream` | Multi-turn streaming chat with 4-tier memory retrieval and SSE response. |
| `GET` | `/api/v1/admin/users` | List tenant users (Admin role required). |
| `GET` | `/api/v1/admin/documents` | List tenant documents and indexing status. |
| `GET` | `/api/v1/admin/analytics` | Retrieve tenant token usage, storage, and cost breakdown. |

---

## 📚 Documentation Index

Detailed architectural specs and execution plans are available in the [`docs/`](./docs) directory:

- 📄 [System Overview (`docs/overview.md`)](./docs/overview.md) — High-level architecture and system components.
- 📐 [Architecture & Detailed Flow (`docs/architecture_and_flow.md`)](./docs/architecture_and_flow.md) — Sequence flows and execution lifecycle.
- 🗄️ [Database Schema & RLS Policies (`docs/database_schema.md`)](./docs/database_schema.md) — PostgreSQL DDL, tables, and security policies.
- 📋 [Implementation Plan (`docs/implementation_plan.md`)](./docs/implementation_plan.md) — Technical implementation specifications.
- 🗺️ [Roadmap & Task Breakdown (`docs/steps_tasks.md`)](./docs/steps_tasks.md) — Phase-by-phase development task checklist.

---

## 📄 License
This project is licensed under the MIT License.
