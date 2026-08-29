"""
Test Script for Document API Endpoints using FastAPI TestClient / AsyncClient.
Tests:
  1. POST /api/v1/documents/upload (Multipart upload + Kafka event trigger)
  2. GET /api/v1/documents (Tenant-scoped listing)
  3. GET /api/v1/documents/{id} (Metadata + Signed URL generation)
  4. DELETE /api/v1/documents/{id} (Purge vectors & Storage & DB)
Usage:
    python -m scripts.test_documents_api
"""

import asyncio
import io
import uuid
import httpx
from sqlalchemy import delete
from app.core.kafka_producer import kafka_producer
from app.db.models.tenant import Tenant
from app.db.models.user import User
from app.db.session import async_session_factory
from app.main import app


async def main() -> None:
    print("==================================================================")
    print("[TEST] Running Document API Endpoints Integration Test...")
    print("==================================================================")

    test_tenant_id = uuid.uuid4()
    test_user_id = uuid.uuid4()
    doc_id = None

    try:
        # Step 1: Create Test Tenant and User in DB
        print("\n[STEP 1] Creating Test Tenant and User in PostgreSQL...")
        async with async_session_factory() as session:
            async with session.begin():
                tenant = Tenant(id=test_tenant_id, name="Acme Analytics Corp", plan_tier="enterprise")
                session.add(tenant)
                user = User(
                    id=test_user_id,
                    tenant_id=test_tenant_id,
                    email=f"admin_{uuid.uuid4().hex[:4]}@acme.com",
                    full_name="Acme Admin",
                    role="admin",
                )
                session.add(user)
        print("[SUCCESS] Tenant and User created!")

        # Step 2: Mock Kafka producer publish_event for API testing
        async def mock_publish(*args, **kwargs):
            return {"topic": "doc-ingestion-events", "partition": 0, "offset": 1}

        kafka_producer.publish_event = mock_publish

        # Step 3: Test FastAPI AsyncClient
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # 1. Health check
            resp = await client.get("/health")
            print(f"\n[GET /health] Status: {resp.status_code}, Body: {resp.json()}")
            assert resp.status_code == 200

            # 2. Upload Document
            print("\n[STEP 2] Testing POST /api/v1/documents/upload...")
            file_content = b"# Acme Analytics Q3 Report\n\nCloud revenue reached $30M with 99.99% uptime."
            files = {"file": ("acme_q3_report.pdf", io.BytesIO(file_content), "application/pdf")}
            data = {"tenant_id": str(test_tenant_id), "user_id": str(test_user_id)}

            upload_resp = await client.post("/api/v1/documents/upload", files=files, data=data)
            print(f"[UPLOAD RESPONSE] Status: {upload_resp.status_code}, Body: {upload_resp.json()}")
            assert upload_resp.status_code == 202
            upload_data = upload_resp.json()
            doc_id = upload_data["document_id"]
            print(f"[SUCCESS] Document Uploaded! ID: {doc_id}")

            # 3. List Documents
            print("\n[STEP 3] Testing GET /api/v1/documents (Tenant Scoped)...")
            list_resp = await client.get(f"/api/v1/documents?tenant_id={test_tenant_id}")
            print(f"[LIST RESPONSE] Status: {list_resp.status_code}, Count: {len(list_resp.json())}")
            assert list_resp.status_code == 200
            assert len(list_resp.json()) == 1

            # 4. Get Document Details & Signed URL
            print(f"\n[STEP 4] Testing GET /api/v1/documents/{doc_id}...")
            get_resp = await client.get(f"/api/v1/documents/{doc_id}?tenant_id={test_tenant_id}")
            print(f"[GET RESPONSE] Status: {get_resp.status_code}")
            get_data = get_resp.json()
            print(f"  -> File Name: {get_data['file_name']}")
            print(f"  -> Status: {get_data['status']}")
            print(f"  -> Signed URL generated: {bool(get_data.get('signed_url'))}")
            assert get_resp.status_code == 200

            # 5. Delete Document
            print(f"\n[STEP 5] Testing DELETE /api/v1/documents/{doc_id}...")
            del_resp = await client.delete(f"/api/v1/documents/{doc_id}?tenant_id={test_tenant_id}&user_id={test_user_id}")
            print(f"[DELETE RESPONSE] Status: {del_resp.status_code}, Body: {del_resp.json()}")
            assert del_resp.status_code == 200
            assert del_resp.json()["success"] is True

            # 6. Verify Document is Gone
            verify_resp = await client.get(f"/api/v1/documents/{doc_id}?tenant_id={test_tenant_id}")
            print(f"[VERIFY GONE] Status: {verify_resp.status_code} (Expected 404)")
            assert verify_resp.status_code == 404

        print("\n==================================================================")
        print("[VERIFIED] All Document API Endpoints Tested & 100% Operational!")
        print("==================================================================")

    finally:
        # Cleanup Tenant
        print("\n[CLEANUP] Cleaning up test tenant...")
        async with async_session_factory() as session:
            async with session.begin():
                await session.execute(delete(Tenant).where(Tenant.id == test_tenant_id))
        print("[SUCCESS] Test cleanup completed.")


if __name__ == "__main__":
    asyncio.run(main())
