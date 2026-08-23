"""
Supabase Storage Service - Tenant-isolated binary document storage and signed URL generation.
"""

import uuid
from typing import Optional
from fastapi import UploadFile
from app.core.config import settings
from app.db.supabase_client import get_supabase_client


class StorageService:
    """Service to handle document storage in Supabase Storage with strict path isolation."""

    def __init__(self, bucket_name: Optional[str] = None) -> None:
        self.bucket_name = bucket_name or settings.SUPABASE_STORAGE_BUCKET

    def build_storage_path(
        self,
        tenant_id: uuid.UUID,
        user_id: uuid.UUID,
        document_id: uuid.UUID,
        file_name: str,
    ) -> str:
        """
        Generates a deterministic tenant-isolated storage path:
        tenants/{tenant_id}/users/{user_id}/docs/{document_id}_{clean_file_name}
        """
        clean_name = file_name.replace(" ", "_").replace("/", "_")
        return f"tenants/{tenant_id}/users/{user_id}/docs/{document_id}_{clean_name}"

    async def upload_file_bytes(
        self,
        file_bytes: bytes,
        storage_path: str,
        content_type: str = "application/pdf",
    ) -> str:
        """
        Uploads raw file bytes to Supabase Storage bucket under the specified path.
        Returns the storage path.
        """
        supabase = get_supabase_client()
        # upload with upsert enabled so duplicate attempts overwrite cleanly
        res = supabase.storage.from_(self.bucket_name).upload(
            path=storage_path,
            file=file_bytes,
            file_options={"content-type": content_type, "upsert": "true"},
        )
        return storage_path

    async def upload_uploadfile(
        self,
        file: UploadFile,
        storage_path: str,
    ) -> str:
        """Uploads a FastAPI UploadFile stream directly to Supabase Storage."""
        contents = await file.read()
        await file.seek(0)
        return await self.upload_file_bytes(
            file_bytes=contents,
            storage_path=storage_path,
            content_type=file.content_type or "application/pdf",
        )

    async def download_file_bytes(self, storage_path: str) -> bytes:
        """Downloads raw binary file bytes from Supabase Storage."""
        supabase = get_supabase_client()
        data = supabase.storage.from_(self.bucket_name).download(storage_path)
        return data

    async def delete_file(self, storage_path: str) -> bool:
        """Deletes a file from Supabase Storage."""
        supabase = get_supabase_client()
        res = supabase.storage.from_(self.bucket_name).remove([storage_path])
        return bool(res)

    async def create_signed_url(self, storage_path: str, expires_in_seconds: int = 3600) -> str:
        """Generates a temporary signed download URL for secure client-side viewing."""
        supabase = get_supabase_client()
        res = supabase.storage.from_(self.bucket_name).create_signed_url(
            storage_path,
            expires_in=expires_in_seconds,
        )
        return res.get("signedURL") or res.get("signedUrl", "")


storage_service = StorageService()
