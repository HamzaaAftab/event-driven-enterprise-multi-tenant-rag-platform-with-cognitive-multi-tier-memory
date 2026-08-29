"""
SSL Context Factory for Aiven Apache Kafka (SASL_SSL / SSL).
Loads Aiven CA PEM certificate directly into SSLContext memory using cadata.
"""

import ssl
from typing import Optional
from app.core.config import settings


def get_kafka_ssl_context() -> Optional[ssl.SSLContext]:
    """
    Creates an SSLContext configured with Aiven CA certificate.
    Loads PEM data directly into memory via cadata (no temporary files).
    """
    if settings.KAFKA_SECURITY_PROTOCOL not in ("SSL", "SASL_SSL"):
        return None

    ssl_context = ssl.create_default_context()
    if settings.KAFKA_CA_CERT:
        ssl_context.load_verify_locations(cadata=settings.KAFKA_CA_CERT.strip())

    return ssl_context
