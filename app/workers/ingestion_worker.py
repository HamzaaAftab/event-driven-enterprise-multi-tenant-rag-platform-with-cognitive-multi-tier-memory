"""
Document Ingestion Worker - Distributed Kafka Consumer for PDF Processing & Vector Indexing.
Flow: Kafka Event -> Download PDF -> LlamaParse -> Hierarchical Chunker -> Pinecone -> PostgreSQL.
"""

import asyncio
import logging
import uuid
from typing import Any, Dict, Optional
from sqlalchemy import select, update
from app.core.config import settings
from app.db.models.audit import AuditLog
from app.db.models.document import Document, DocumentChunk
from app.db.session import async_session_factory
from app.schemas.events import DocIngestionPayload
from app.services.chunker_service import chunker_service
from app.services.parser_service import parser_service
from app.services.storage_service import storage_service
from app.services.vector_service import vector_service
from app.workers.base_worker import BaseKafkaWorker

logger = logging.getLogger("ingestion_worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class DocumentIngestionWorker(BaseKafkaWorker):
    """
    Consumes document upload events from Kafka, downloads binary files from Supabase Storage,
    parses structured tables using LlamaParse, generates semantic chunks, embeds and upserts
    into tenant-isolated Pinecone namespaces, and commits relational metadata into PostgreSQL.
    """

    def __init__(self) -> None:
        super().__init__(
            topic=settings.KAFKA_TOPIC_INGESTION,
            group_id="doc-ingestion-workers",
            max_retries=3,
            auto_offset_reset="earliest",
        )

    async def _update_doc_status(
        self,
        document_id: uuid.UUID,
        status: str,
        error_message: Optional[str] = None,
        page_count: Optional[int] = None,
        chunk_count: Optional[int] = None,
        metadata_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Updates document processing status in PostgreSQL database."""
        async with async_session_factory() as session:
            async with session.begin():
                values: Dict[str, Any] = {"status": status}
                if error_message is not None:
                    values["error_message"] = error_message
                if page_count is not None:
                    values["page_count"] = page_count
                if chunk_count is not None:
                    values["chunk_count"] = chunk_count
                if metadata_info is not None:
                    values["metadata_info"] = metadata_info

                stmt = update(Document).where(Document.id == document_id).values(**values)
                await session.execute(stmt)

    async def process_message(self, payload: Dict[str, Any], key: Optional[str]) -> None:
        """
        Executes end-to-end ingestion pipeline for a single document event.
        """
        # 1. Parse Event Payload
        raw_payload = payload.get("payload", payload)
        event_data = DocIngestionPayload.model_validate(raw_payload)

        tenant_id = event_data.tenant_id
        user_id = event_data.user_id
        document_id = event_data.document_id
        file_name = event_data.file_name
        storage_path = event_data.storage_path

        logger.info(
            "[INGESTION WORKER] Processing doc '%s' (ID: %s, Tenant: %s)...",
            file_name,
            document_id,
            tenant_id,
        )

        try:
            # 2. Update Status -> parsing
            await self._update_doc_status(document_id, status="parsing")

            # 3. Download Binary from Supabase Storage
            logger.info("[INGESTION] Downloading binary from Supabase Storage: %s", storage_path)
            file_bytes = await storage_service.download_file_bytes(storage_path)
            if not file_bytes:
                raise ValueError(f"Downloaded file bytes are empty for path '{storage_path}'")

            # 4. Parse via LlamaParse
            logger.info("[INGESTION] Parsing document with LlamaParse...")
            parse_result = await parser_service.parse_bytes(file_bytes=file_bytes, file_name=file_name)

            # 5. Update Status -> chunking
            await self._update_doc_status(
                document_id,
                status="chunking",
                page_count=parse_result.total_pages,
            )

            # 6. Hierarchical & Table-Preserving Chunking
            logger.info("[INGESTION] Chunking document with table preservation...")
            chunks = chunker_service.chunk_parse_result(parse_result)
            if not chunks:
                raise ValueError(f"No chunks generated for document '{file_name}'")

            # 7. Update Status -> indexing
            await self._update_doc_status(
                document_id,
                status="indexing",
                chunk_count=len(chunks),
            )

            # 8. Embed & Upsert to Tenant-Isolated Pinecone Namespace
            logger.info(
                "[INGESTION] Vectorizing and upserting %d chunks into Pinecone namespace 'tenant_%s_docs'...",
                len(chunks),
                tenant_id,
            )
            vector_ids = await vector_service.upsert_document_chunks(
                tenant_id=tenant_id,
                document_id=document_id,
                chunks=chunks,
            )

            # 9. Commit DocumentChunks and Status -> indexed in PostgreSQL
            async with async_session_factory() as session:
                async with session.begin():
                    # Create relational DocumentChunk records
                    db_chunks = []
                    for chunk, vec_id in zip(chunks, vector_ids):
                        db_chunk = DocumentChunk(
                            id=uuid.uuid4(),
                            document_id=document_id,
                            tenant_id=tenant_id,
                            chunk_index=chunk.chunk_index,
                            page_number=chunk.page_number,
                            token_count=chunk.token_count,
                            content_preview=chunk.content[:500],
                            pinecone_vector_id=vec_id,
                            metadata_info=chunk.metadata,
                        )
                        db_chunks.append(db_chunk)

                    session.add_all(db_chunks)

                    # Update Document status to indexed
                    doc_stmt = (
                        update(Document)
                        .where(Document.id == document_id)
                        .values(
                            status="indexed",
                            page_count=parse_result.total_pages,
                            chunk_count=len(chunks),
                            error_message=None,
                            metadata_info=parse_result.metadata,
                        )
                    )
                    await session.execute(doc_stmt)

                    # Record Audit Log
                    audit = AuditLog(
                        id=uuid.uuid4(),
                        tenant_id=tenant_id,
                        user_id=user_id,
                        action="DOC_INGEST_COMPLETE",
                        resource_type="document",
                        resource_id=str(document_id),
                        details={
                            "file_name": file_name,
                            "page_count": parse_result.total_pages,
                            "chunk_count": len(chunks),
                            "pinecone_namespace": f"tenant_{tenant_id}_docs",
                        },
                    )
                    session.add(audit)

            logger.info(
                "[INGESTION SUCCESS] Document '%s' (ID: %s) is now fully INDEXED with %d chunks!",
                file_name,
                document_id,
                len(chunks),
            )

        except Exception as err:
            logger.error("[INGESTION FAILED] Failed processing document '%s': %s", file_name, err)
            # Mark document failed in DB
            try:
                await self._update_doc_status(
                    document_id=document_id,
                    status="failed",
                    error_message=str(err),
                )
            except Exception as db_err:
                logger.error("[INGESTION DB ERROR] Could not mark document as failed: %s", db_err)
            raise


async def run_worker() -> None:
    """Entrypoint to run ingestion worker process."""
    worker = DocumentIngestionWorker()
    logger.info("Starting Document Ingestion Worker...")
    await worker.start()


if __name__ == "__main__":
    asyncio.run(run_worker())
