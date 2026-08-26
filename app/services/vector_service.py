"""
Vector Service - Universal Embedding Generation & Tenant-Isolated Pinecone Vector Store.
Enforces strict namespace separation (tenant_{tenant_id}_docs & tenant_{tenant_id}_memory).
"""

import asyncio
import logging
import uuid
from typing import Any, Dict, List, Optional
import httpx
from pinecone import Pinecone
from app.core.config import settings
from app.services.chunker_service import ChunkItem

logger = logging.getLogger("vector_service")


class VectorService:
    """
    Manages vector embeddings and multi-tenant isolated Pinecone indexing.
    - Generates 2048-dimensional embeddings matching Pinecone index configuration.
    - Routes all vector operations into isolated tenant namespaces.
    """

    def __init__(self) -> None:
        self._pc: Optional[Pinecone] = None
        self._index = None
        self.dimension = settings.EMBEDDING_DIMENSION

    def _get_index(self):
        """Lazily initializes the Pinecone client and index connection."""
        if self._index is None:
            if not settings.PINECONE_API_KEY:
                raise ValueError("PINECONE_API_KEY is not configured in .env")
            self._pc = Pinecone(api_key=settings.PINECONE_API_KEY)
            self._index = self._pc.Index(settings.PINECONE_INDEX_NAME)
        return self._index

    # =========================================================================
    # 1. NAMESPACE ISOLATION HELPERS
    # =========================================================================
    @staticmethod
    def get_docs_namespace(tenant_id: uuid.UUID) -> str:
        """Returns the isolated document namespace for a tenant."""
        return f"tenant_{tenant_id}_docs"

    @staticmethod
    def get_memory_namespace(tenant_id: uuid.UUID) -> str:
        """Returns the isolated episodic memory namespace for a tenant."""
        return f"tenant_{tenant_id}_memory"

    # =========================================================================
    # 2. UNIVERSAL EMBEDDING GENERATION
    # =========================================================================
    async def generate_embeddings(
        self,
        texts: List[str],
        batch_size: int = 16,
    ) -> List[List[float]]:
        """
        Generates dense vector embeddings using OpenRouter / OpenAI embeddings API.
        Automatically batches inputs and guarantees 2048 dimensions.
        """
        if not texts:
            return []

        all_embeddings: List[List[float]] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for i in range(0, len(texts), batch_size):
                batch_texts = texts[i : i + batch_size]
                # Replace newlines with spaces for optimal embedding representations
                cleaned_batch = [t.replace("\n", " ").strip() for t in batch_texts]

                try:
                    # OpenRouter Embedding with 2048 target dimensions
                    response = await client.post(
                        f"{settings.OPENROUTER_BASE_URL.rstrip('/')}/embeddings",
                        headers={
                            "Authorization": f"Bearer {settings.OPENROUTER_API_KEY}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": "openai/text-embedding-3-large",
                            "input": cleaned_batch,
                            "dimensions": self.dimension,
                        },
                    )

                    if response.status_code != 200:
                        raise RuntimeError(
                            f"Embedding API error ({response.status_code}): {response.text}"
                        )

                    data = response.json()
                    for item in data.get("data", []):
                        vec = item.get("embedding", [])
                        # Ensure dimension matches target
                        if len(vec) < self.dimension:
                            vec = vec + [0.0] * (self.dimension - len(vec))
                        elif len(vec) > self.dimension:
                            vec = vec[: self.dimension]
                        all_embeddings.append(vec)

                except Exception as e:
                    logger.error("[EMBEDDING ERROR] Failed batch embedding: %s", e)
                    raise

        return all_embeddings

    async def generate_query_embedding(self, query: str) -> List[float]:
        """Generates embedding vector for a search query string."""
        results = await self.generate_embeddings([query])
        if not results:
            raise RuntimeError("Failed to generate embedding for query.")
        return results[0]

    # =========================================================================
    # 3. PINECONE INDEXING & SEARCH OPERATIONS (TENANT-SCOPED)
    # =========================================================================
    async def upsert_document_chunks(
        self,
        tenant_id: uuid.UUID,
        document_id: uuid.UUID,
        chunks: List[ChunkItem],
    ) -> List[str]:
        """
        Embeds and upserts document chunks into the tenant's isolated Pinecone namespace.
        Returns a list of pinecone vector IDs for relational database tracking.
        """
        if not chunks:
            return []

        namespace = self.get_docs_namespace(tenant_id)
        logger.info(
            "[VECTOR UPSERT] Upserting %d chunks for doc '%s' into namespace '%s'...",
            len(chunks),
            document_id,
            namespace,
        )

        # 1. Extract texts and generate embeddings
        chunk_texts = [c.content for c in chunks]
        embeddings = await self.generate_embeddings(chunk_texts)

        # 2. Build Pinecone vector records with rich metadata
        vectors_to_upsert = []
        vector_ids: List[str] = []

        for chunk, emb in zip(chunks, embeddings):
            vector_id = f"doc_{document_id}_chunk_{chunk.chunk_index}"
            vector_ids.append(vector_id)

            metadata = {
                "tenant_id": str(tenant_id),
                "document_id": str(document_id),
                "chunk_index": chunk.chunk_index,
                "page_number": chunk.page_number,
                "is_table": chunk.is_table,
                "token_count": chunk.token_count,
                "section_h1": chunk.metadata.get("section_h1", ""),
                "section_h2": chunk.metadata.get("section_h2", ""),
                "file_name": chunk.metadata.get("file_name", ""),
                "content": chunk.content[:1000],  # Pinecone metadata payload limit safety
            }

            vectors_to_upsert.append(
                {
                    "id": vector_id,
                    "values": emb,
                    "metadata": metadata,
                }
            )

        # 3. Upsert in batches of 50
        index = self._get_index()
        batch_size = 50
        for i in range(0, len(vectors_to_upsert), batch_size):
            batch = vectors_to_upsert[i : i + batch_size]
            # Execute synchronous Pinecone call in threadpool
            await asyncio.to_thread(
                index.upsert,
                vectors=batch,
                namespace=namespace,
            )

        logger.info("[VECTOR SUCCESS] Upserted %d vectors into namespace '%s'", len(vector_ids), namespace)
        return vector_ids

    async def search_documents(
        self,
        tenant_id: uuid.UUID,
        query: str,
        top_k: int = 5,
        filter_document_id: Optional[uuid.UUID] = None,
    ) -> List[Dict[str, Any]]:
        """
        Executes similarity search strictly isolated to the tenant's document namespace.
        Optional filter by specific document_id.
        """
        namespace = self.get_docs_namespace(tenant_id)
        query_embedding = await self.generate_query_embedding(query)

        filter_dict = {}
        if filter_document_id:
            filter_dict["document_id"] = str(filter_document_id)

        index = self._get_index()
        query_kwargs = {
            "vector": query_embedding,
            "top_k": top_k,
            "namespace": namespace,
            "include_metadata": True,
        }
        if filter_dict:
            query_kwargs["filter"] = filter_dict

        # Run query in threadpool
        response = await asyncio.to_thread(index.query, **query_kwargs)

        results = []
        for match in response.matches:
            results.append(
                {
                    "vector_id": match.id,
                    "score": match.score,
                    "metadata": match.metadata,
                    "content": match.metadata.get("content", ""),
                    "page_number": match.metadata.get("page_number", 1),
                    "file_name": match.metadata.get("file_name", ""),
                    "is_table": match.metadata.get("is_table", False),
                }
            )

        return results

    async def delete_document_vectors(
        self,
        tenant_id: uuid.UUID,
        vector_ids: List[str],
    ) -> None:
        """Deletes vectors from a tenant's document namespace."""
        if not vector_ids:
            return
        namespace = self.get_docs_namespace(tenant_id)
        index = self._get_index()
        await asyncio.to_thread(index.delete, ids=vector_ids, namespace=namespace)
        logger.info("[VECTOR DELETED] Deleted %d vectors from namespace '%s'", len(vector_ids), namespace)


vector_service = VectorService()
