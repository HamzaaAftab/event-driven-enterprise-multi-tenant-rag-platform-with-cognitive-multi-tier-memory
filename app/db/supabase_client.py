"""
Supabase Client Singleton & Storage Helper.
Provides authenticated access to Supabase Storage and database RPCs.
"""

from typing import Optional
from supabase import Client, ClientOptions, create_client
from app.core.config import settings


class SupabaseService:
    """Wrapper around Supabase Python Client."""

    def __init__(self) -> None:
        self._client: Optional[Client] = None

    @property
    def client(self) -> Client:
        """Lazily initialize and return the Supabase client."""
        if self._client is None:
            # Use Service Role Key for server-side elevated operations, fallback to Anon key
            api_key = settings.SUPABASE_SERVICE_ROLE_KEY or settings.SUPABASE_ANON_KEY
            if not settings.SUPABASE_URL or not api_key:
                raise ValueError(
                    "Supabase URL or API Key is missing. Check SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env"
                )
            clean_url = settings.SUPABASE_URL.rstrip("/").removesuffix("/rest/v1")
            self._client = create_client(
                clean_url,
                api_key,
                ClientOptions(schema="public", auto_refresh_token=False, persist_session=False),
            )
        return self._client


supabase_service = SupabaseService()


def get_supabase_client() -> Client:
    """Helper function to obtain the Supabase client instance."""
    return supabase_service.client
