"""
Document API Router - Multipart PDF Upload, Tenant-Scoped Document Management & Vector Deletion.
"""

import logging
import os
import uuid
from typing import List, Optional
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.core.config import settings
from app.core.kafka_producer import kafka_producer
from app.db.models.audit import AuditLog
from app.db.models.document import Document, DocumentChunk
from app.db.models.tenant import Tenant
from app.db.models.user import User
from app.db.session import get_async_session
from app.schemas.document import DocumentDetailRead, DocumentRead, DocumentUploadResponse
from app.schemas.events import DocIngestionEvent, DocIngestionPayload
from app.services.storage_service import storage_service
from app.services.vector_service import vector_service

logger = logging.getLogger("documents_api")
router = APIRouter(prefix="/documents", tags=["Documents & Ingestion"])

ALLOWED_EXTENSIONS = {".pdf", ".txt", ".md", ".docx", ".csv"}
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB


@router.post(
    "/upload",
    response_model=DocumentUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Upload Document for Multi-Tenant RAG Ingestion",
    description=(
        "Uploads a binary document (PDF/MD/TXT) into tenant-isolated Supabase Storage, "
        "registers the document in PostgreSQL, and queues an asynchronous event in Kafka."
    ),
)
async def upload_document(
    file: UploadFile = File(..., description="Binary document to be indexed"),
    tenant_id: uuid.UUID = Form(..., description="Target Tenant/Organization ID"),
    user_id: uuid.UUID = Form(..., description="Uploading User ID"),
    db: AsyncSession = Depends(get_async_session),
) -> DocumentUploadResponse:
    """Handles multipart document upload with storage partitioning and Kafka event routing."""
    # 1. Validate File Extension
    file_name = file.filename or "uploaded_doc.pdf"
    ext = os.path.splitext(file_name)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported file type '{ext}'. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        )

    # 2. Validate Tenant & User Existence
    tenant = await db.get(Tenant, tenant_id)
    if not tenant:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found.")

    user = await db.get(User, user_id)
    if not user or user.tenant_id != tenant_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="User does not belong to the specified tenant.",
        )

    # 3. Read Binary Stream
    file_bytes = await file.read()
    file_size = len(file_bytes)
    if file_size == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Uploaded file is empty.")
    if file_size > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File exceeds maximum allowed size of {MAX_FILE_SIZE_BYTES // (1024 * 1024)}MB.",
        )

    document_id = uuid.uuid4()
    storage_path = storage_service.build_storage_path(
        tenant_id=tenant_id,
        user_id=user_id,
        document_id=document_id,
        file_name=file_name,
    )

    try:
        # 4. Upload File to Supabase Storage
        logger.info("[UPLOAD API] Uploading '%s' to storage path '%s'...", file_name, storage_path)
        await storage_service.upload_file_bytes(
            file_bytes=file_bytes,
            storage_path=storage_path,
            content_type=file.content_type or "application/pdf",
        )

        # 5. Insert Document in PostgreSQL
        doc = Document(
            id=document_id,
            tenant_id=tenant_id,
            user_id=user_id,
            file_name=file_name,
            storage_path=storage_path,
            file_size_bytes=file_size,
            mime_type=file.content_type or "application/pdf",
            status="pending",
            metadata_info={"original_name": file_name, "uploaded_by": str(user_id)},
        )
        db.add(doc)

        # Record Initial Upload Audit Log
        audit = AuditLog(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            user_id=user_id,
            action="DOC_UPLOAD_INITIATED",
            resource_type="document",
            resource_id=str(document_id),
            details={"file_name": file_name, "file_size_bytes": file_size},
        )
        db.add(audit)
        await db.commit()

        # 6. Publish Event to Kafka (Partition Key = tenant_id)
        event = DocIngestionEvent(
            payload=DocIngestionPayload(
                tenant_id=tenant_id,
                user_id=user_id,
                document_id=document_id,
                file_name=file_name,
                storage_path=storage_path,
                mime_type=file.content_type or "application/pdf",
            )
        )
        await kafka_producer.publish_event(
            topic=settings.KAFKA_TOPIC_INGESTION,
            event=event,
            key=tenant_id,
        )

        logger.info("[UPLOAD API SUCCESS] Doc '%s' queued into Kafka topic '%s'", file_name, settings.KAFKA_TOPIC_INGESTION)

        return DocumentUploadResponse(
            document_id=document_id,
            file_name=file_name,
            storage_path=storage_path,
            status="pending",
            message="Document uploaded successfully and queued for background ingestion.",
        )

    except Exception as e:
        logger.error("[UPLOAD API ERROR] Failed uploading document: %s", e)
        # Attempt to clean storage file on failure
        try:
            await storage_service.delete_file(storage_path)
        except Exception:
            pass
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process document upload: {e}",
        )


@router.get(
    "",
    response_model=List[DocumentRead],
    summary="List Tenant Documents",
    description="Retrieves a paginated list of documents strictly filtered by tenant.",
)
async def list_documents(
    tenant_id: uuid.UUID = Query(..., description="Tenant ID to scope query"),
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status (indexed, parsing, failed)"),
    skip: int = Query(0, ge=0, description="Offset for pagination"),
    limit: int = Query(20, ge=1, le=100, description="Limit per page"),
    db: AsyncSession = Depends(get_async_session),
) -> List[DocumentRead]:
    """Lists all documents for a tenant with optional status filter."""
    stmt = (
        select(Document)
        .where(Document.tenant_id == tenant_id)
        .order_by(Document.created_at.desc())
        .offset(skip)
        .limit(limit)
    )
    if status_filter:
        stmt = stmt.where(Document.status == status_filter)

    result = await db.execute(stmt)
    documents = result.scalars().all()
    return [DocumentRead.model_validate(d) for d in documents]


@router.get(
    "/{document_id}",
    response_model=DocumentDetailRead,
    summary="Get Document by ID with Signed URL",
    description="Fetches document details, status, chunk counts, and a secure temporary signed download URL.",
)
async def get_document(
    document_id: uuid.UUID,
    tenant_id: uuid.UUID = Query(..., description="Tenant ID for security validation"),
    db: AsyncSession = Depends(get_async_session),
) -> DocumentDetailRead:
    """Gets document metadata and generates temporary signed URL."""
    stmt = (
        select(Document)
        .options(selectinload(Document.chunks))
        .where(Document.id == document_id, Document.tenant_id == tenant_id)
    )
    result = await db.execute(stmt)
    doc = result.scalar_one_or_none()
    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found.")

    doc_read = DocumentDetailRead.model_validate(doc)
    try:
        # Generate temporary signed URL for file access
        doc_read.signed_url = await storage_service.create_signed_url(doc.storage_path, expires_in_seconds=3600)
    except Exception as err:
        logger.warning("[SIGNED URL WARN] Could not generate signed URL: %s", err)

    return doc_read


@router.delete(
    "/{document_id}",
    status_code=status.HTTP_200_OK,
    summary="Delete Document & Purge Vectors",
    description="Deletes document from Supabase Storage, purges all chunk vectors from Pinecone, and removes PostgreSQL records.",
)
async def delete_document(
    document_id: uuid.UUID,
    tenant_id: uuid.UUID = Query(..., description="Tenant ID for tenant scoping"),
    user_id: Optional[uuid.UUID] = Query(None, description="User executing the deletion"),
    db: AsyncSession = Depends(get_async_session),
) -> dict:
    """Purges a document, its storage binary, and all indexed vectors in Pinecone."""
    # 1. Fetch document and associated vector IDs
    stmt = (
        select(Document)
        .options(selectinload(Document.chunks))
        .where(Document.id == document_id, Document.tenant_id == tenant_id)
    )
    result = await db.execute(stmt)
    doc = result.scalar_one_or_none()
    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found.")

    storage_path = doc.storage_path
    vector_ids = [c.pinecone_vector_id for c in doc.chunks if c.pinecone_vector_id]

    try:
        # 2. Delete vectors from Pinecone namespace
        if vector_ids:
            logger.info("[DELETE API] Purging %d vectors from Pinecone namespace 'tenant_%s_docs'...", len(vector_ids), tenant_id)
            await vector_service.delete_document_vectors(tenant_id=tenant_id, vector_ids=vector_ids)

        # 3. Delete binary from Supabase Storage
        logger.info("[DELETE API] Deleting storage file '%s'...", storage_path)
        await storage_service.delete_file(storage_path)

        # 4. Record Audit Log
        audit = AuditLog(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            user_id=user_id,
            action="DOC_DELETE",
            resource_type="document",
            resource_id=str(document_id),
            details={"file_name": doc.file_name, "purged_vectors_count": len(vector_ids)},
        )
        db.add(audit)

        # 5. Delete Document in DB (Cascade deletes DocumentChunks)
        await db.delete(doc)
        await db.commit()

        logger.info("[DELETE API SUCCESS] Document '%s' (ID: %s) completely purged.", doc.file_name, document_id)

        return {
            "success": True,
            "message": "Document and all associated vector embeddings successfully deleted.",
            "document_id": str(document_id),
        }

    except Exception as e:
        logger.error("[DELETE API ERROR] Failed deleting document '%s': %s", document_id, e)
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete document: {e}",
        )
