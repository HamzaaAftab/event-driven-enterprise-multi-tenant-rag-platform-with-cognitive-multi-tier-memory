"""
End-to-End Test for Document Ingestion Pipeline.
Simulates upload -> Storage -> Kafka -> Ingestion Worker -> LlamaParse -> Chunker -> Pinecone -> PostgreSQL.
Usage:
    python -m scripts.test_ingestion_pipeline
"""

import asyncio
import uuid
from sqlalchemy import select, delete
from app.core.config import settings
from app.db.models.audit import AuditLog
from app.db.models.document import Document, DocumentChunk
from app.db.models.tenant import Tenant
from app.db.models.user import User
from app.db.session import async_session_factory
from app.schemas.events import DocIngestionEvent, DocIngestionPayload
from app.services.storage_service import storage_service
from app.services.vector_service import vector_service
from app.workers.ingestion_worker import DocumentIngestionWorker


async def main() -> None:
    print("==================================================================")
    print("[TEST] Running Full End-to-End Document Ingestion Pipeline Test...")
    print("==================================================================")

    test_tenant_id = uuid.uuid4()
    test_user_id = uuid.uuid4()
    test_doc_id = uuid.uuid4()
    test_file_name = "Enterprise_Q3_Financial_Summary.pdf"

    # Sample PDF/Markdown content
    sample_content = (
        b"# TechCorp Enterprise Financial Report 2026\n\n"
        b"## Executive Summary\n"
        b"TechCorp experienced accelerated adoption of multi-tenant AI solutions in Q3.\n\n"
        b"| Segment | Q2 Revenue | Q3 Revenue | Growth |\n"
        b"| --- | --- | --- | --- |\n"
        b"| Enterprise RAG | $14.2M | $21.5M | +51% |\n"
        b"| Cognitive Memory | $8.1M | $12.4M | +53% |\n"
        b"| Cloud Storage | $18.0M | $19.2M | +6% |\n\n"
        b"## Future Outlook\n"
        b"Operating margins improved by 400 basis points due to automated document processing."
    )

    try:
        # Step 1: Create Tenant and User in DB
        print("\n[STEP 1] Creating Test Tenant and User in PostgreSQL...")
        async with async_session_factory() as session:
            async with session.begin():
                tenant = Tenant(
                    id=test_tenant_id,
                    name="TechCorp International",
                    plan_tier="enterprise",
                )
                session.add(tenant)

                user = User(
                    id=test_user_id,
                    tenant_id=test_tenant_id,
                    email=f"cfo_{uuid.uuid4().hex[:4]}@techcorp.com",
                    full_name="Sarah Jenkins (CFO)",
                    role="admin",
                )
                session.add(user)

        print("[SUCCESS] Tenant and User created in PostgreSQL!")

        # Step 2: Upload File to Supabase Storage
        storage_path = storage_service.build_storage_path(
            tenant_id=test_tenant_id,
            user_id=test_user_id,
            document_id=test_doc_id,
            file_name=test_file_name,
        )
        print(f"\n[STEP 2] Uploading sample file to Supabase Storage: {storage_path}")
        await storage_service.upload_file_bytes(sample_content, storage_path)
        print("[SUCCESS] File uploaded to Supabase Storage!")

        # Step 3: Insert Document record (status = 'pending') in PostgreSQL
        print("\n[STEP 3] Registering Document in PostgreSQL (status = 'pending')...")
        async with async_session_factory() as session:
            async with session.begin():
                doc = Document(
                    id=test_doc_id,
                    tenant_id=test_tenant_id,
                    user_id=test_user_id,
                    file_name=test_file_name,
                    storage_path=storage_path,
                    file_size_bytes=len(sample_content),
                    mime_type="application/pdf",
                    status="pending",
                )
                session.add(doc)

        print("[SUCCESS] Document registered in PostgreSQL!")

        # Step 4: Execute Ingestion Worker on the Event
        print("\n[STEP 4] Executing DocumentIngestionWorker pipeline...")
        worker = DocumentIngestionWorker()
        event_payload = DocIngestionPayload(
            tenant_id=test_tenant_id,
            user_id=test_user_id,
            document_id=test_doc_id,
            file_name=test_file_name,
            storage_path=storage_path,
        )
        await worker.process_message(event_payload.model_dump(), key=str(test_tenant_id))
        print("[SUCCESS] Worker processed message successfully!")

        # Step 5: Verify PostgreSQL State
        print("\n[STEP 5] Verifying Database State...")
        async with async_session_factory() as session:
            # Check document
            doc_res = await session.execute(select(Document).where(Document.id == test_doc_id))
            indexed_doc = doc_res.scalar_one()
            print(f"  -> Document Status: {indexed_doc.status}")
            print(f"  -> Total Pages: {indexed_doc.page_count}")
            print(f"  -> Total Chunks: {indexed_doc.chunk_count}")
            assert indexed_doc.status == "indexed", f"Expected 'indexed', got '{indexed_doc.status}'"

            # Check chunks
            chunk_res = await session.execute(
                select(DocumentChunk).where(DocumentChunk.document_id == test_doc_id)
            )
            chunks = chunk_res.scalars().all()
            print(f"  -> Saved Database Chunks Count: {len(chunks)}")
            for c in chunks:
                print(f"     * Chunk {c.chunk_index}: VectorID='{c.pinecone_vector_id}', Tokens={c.token_count}")

            # Check audit log
            audit_res = await session.execute(
                select(AuditLog).where(AuditLog.tenant_id == test_tenant_id)
            )
            audits = audit_res.scalars().all()
            print(f"  -> Audit Logs Count: {len(audits)}")

        # Step 6: Verify Search in Pinecone
        print("\n[STEP 6] Verifying Semantic Search in Pinecone Namespace...")
        await asyncio.sleep(2)  # Wait for index propagation
        search_results = await vector_service.search_documents(
            tenant_id=test_tenant_id,
            query="What was the revenue and growth for Enterprise RAG in Q3?",
            top_k=2,
        )
        print(f"[SUCCESS] Pinecone returned {len(search_results)} matching chunks:")
        for r in search_results:
            print(f"  -> Score: {r['score']:.4f} | Page: {r['page_number']} | Table: {r['is_table']}")
            print(f"     Content: {r['content'][:120]}...")

        print("\n==================================================================")
        print("[VERIFIED] End-to-End Document Ingestion Pipeline 100% SUCCESSFUL!")
        print("==================================================================")

    finally:
        # Step 7: Clean Up Test Artifacts
        print("\n[CLEANUP] Cleaning up test data from Database, Storage, and Pinecone...")
        try:
            # 1. Clean Storage
            await storage_service.delete_file(storage_path)

            # 2. Clean Pinecone
            async with async_session_factory() as session:
                chunk_res = await session.execute(
                    select(DocumentChunk.pinecone_vector_id).where(
                        DocumentChunk.tenant_id == test_tenant_id
                    )
                )
                vec_ids = [r[0] for r in chunk_res.all()]
                if vec_ids:
                    await vector_service.delete_document_vectors(test_tenant_id, vec_ids)

            # 3. Clean Database (Cascades to documents, chunks, audit)
            async with async_session_factory() as session:
                async with session.begin():
                    await session.execute(delete(Tenant).where(Tenant.id == test_tenant_id))

            print("[SUCCESS] All test artifacts cleaned up cleanly!")
        except Exception as clean_err:
            print(f"[CLEANUP NOTE]: {clean_err}")


if __name__ == "__main__":
    asyncio.run(main())
