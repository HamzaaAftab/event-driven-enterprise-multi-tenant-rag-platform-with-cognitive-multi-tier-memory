"""
SSL Context Factory for Aiven Apache Kafka (SASL_SSL / SSL).
Handles dynamic CA certificate loading safely across Windows and Linux.
"""

import os
import ssl
import tempfile
from typing import Optional
from app.core.config import settings


def get_kafka_ssl_context() -> Optional[ssl.SSLContext]:
    """
    Creates an SSLContext configured with Aiven CA certificate.
    If KAFKA_CA_CERT is provided in settings, creates a secure SSL context with cert validation.
    """
    if settings.KAFKA_SECURITY_PROTOCOL not in ("SSL", "SASL_SSL"):
        return None

    if not settings.KAFKA_CA_CERT:
        # Fall back to default system CA roots
        return ssl.create_default_context()

    # Create temporary file to load CA pem into OpenSSL context
    temp_ca = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem")
    try:
        temp_ca.write(settings.KAFKA_CA_CERT.strip())
        temp_ca.close()
        ssl_context = ssl.create_default_context(cafile=temp_ca.name)
        return ssl_context
    finally:
        if os.path.exists(temp_ca.name):
            try:
                os.unlink(temp_ca.name)
            except Exception:
                pass
