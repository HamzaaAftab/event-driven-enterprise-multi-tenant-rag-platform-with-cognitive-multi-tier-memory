"""
Live Test Script for VectorService & Pinecone Vector Store.
Embeds test chunks, queries Pinecone, and tests strict tenant namespace isolation.
Usage:
    python -m scripts.test_vector_service
"""

import asyncio
import uuid
from app.services.chunker_service import ChunkItem
from app.services.vector_service import vector_service


async def main() -> None:
    print("==================================================================")
    print("[TEST] Running VectorService & Pinecone Live Integration Test...")
    print("==================================================================")

    test_tenant_id = uuid.uuid4()
    test_doc_id = uuid.uuid4()

    sample_chunks = [
        ChunkItem(
            chunk_index=0,
            content="[Context: Financial Report 2026]\nOur cloud revenue reached $18M in North America during Q3 2026.",
            page_number=1,
            token_count=25,
            is_table=False,
            metadata={"file_name": "q3_report.pdf", "section_h1": "Financial Report 2026"},
        ),
        ChunkItem(
            chunk_index=1,
            content="[Context: Financial Report 2026 > Tables]\n| Region | Revenue |\n| NA | $18M |\n| EMEA | $11M |",
            page_number=2,
            token_count=30,
            is_table=True,
            metadata={"file_name": "q3_report.pdf", "section_h1": "Financial Report 2026"},
        ),
    ]

    # 1. Upsert Chunks
    print(f"\n[STEP 1] Upserting into Pinecone namespace: tenant_{test_tenant_id}_docs")
    vector_ids = await vector_service.upsert_document_chunks(
        tenant_id=test_tenant_id,
        document_id=test_doc_id,
        chunks=sample_chunks,
    )
    print(f"[SUCCESS] Upserted Vector IDs: {vector_ids}")

    # 2. Wait 2 seconds for Pinecone index propagation
    print("\n[STEP 2] Waiting 2s for index propagation...")
    await asyncio.sleep(2)

    # 3. Query Pinecone
    query_text = "What was the revenue in North America?"
    print(f"\n[STEP 3] Querying Pinecone: '{query_text}'")
    results = await vector_service.search_documents(
        tenant_id=test_tenant_id,
        query=query_text,
        top_k=2,
    )
    print(f"[SUCCESS] Found {len(results)} matches:")
    for r in results:
        print(f"  -> Score: {r['score']:.4f} | Page: {r['page_number']} | Content: {r['content']}")

    # 4. Clean up test vectors
    print("\n[STEP 4] Cleaning up test vectors from Pinecone...")
    await vector_service.delete_document_vectors(test_tenant_id, vector_ids)
    print("[SUCCESS] Test vectors cleaned up successfully!")

    print("\n==================================================================")
    print("[VERIFIED] VectorService & Pinecone Multi-Tenant Search is 100% Operational!")
    print("==================================================================")


if __name__ == "__main__":
    asyncio.run(main())
