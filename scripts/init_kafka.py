"""
Kafka Initialization Script - Creates required topics in Aiven Apache Kafka cluster.
Usage:
    python -m scripts.init_kafka
"""

import asyncio
import os
import ssl
import sys
import tempfile
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from app.core.config import settings


async def init_kafka_topics() -> None:
    """Initializes required Kafka topics in Aiven cluster."""
    print("==================================================================")
    print("[INIT] Initializing Aiven Apache Kafka Topics...")
    print(f"[BOOTSTRAP] Broker: {settings.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"[SECURITY] Protocol: {settings.KAFKA_SECURITY_PROTOCOL} | Mechanism: {settings.KAFKA_SASL_MECHANISM}")
    print("==================================================================")

    # 1. Setup SSL Context from CA Certificate string
    temp_ca_path = None
    ssl_context = None
    if settings.KAFKA_CA_CERT:
        temp_ca = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".pem")
        temp_ca.write(settings.KAFKA_CA_CERT)
        temp_ca.close()
        temp_ca_path = temp_ca.name
        ssl_context = ssl.create_default_context(cafile=temp_ca_path)

    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol=settings.KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=settings.KAFKA_SASL_MECHANISM,
        sasl_plain_username=settings.KAFKA_SASL_USERNAME,
        sasl_plain_password=settings.KAFKA_SASL_PASSWORD,
        ssl_context=ssl_context,
    )

    try:
        print("[CONNECT] Connecting to Kafka cluster...")
        await admin_client.start()
        print("[SUCCESS] Connected to Aiven Kafka Admin!")

        # Define desired topics
        topics_to_create = [
            NewTopic(
                name=settings.KAFKA_TOPIC_INGESTION,
                num_partitions=2,
                replication_factor=1,
            ),
            NewTopic(
                name=settings.KAFKA_TOPIC_MEMORY,
                num_partitions=2,
                replication_factor=1,
            ),
            NewTopic(
                name=settings.KAFKA_TOPIC_DLQ,
                num_partitions=1,
                replication_factor=1,
            ),
        ]

        # Create topics
        try:
            res = await admin_client.create_topics(topics_to_create)
            print(f"[SUCCESS] create_topics executed: {res}")
        except TopicAlreadyExistsError:
            print("[INFO] One or more topics already exist.")
        except Exception as e:
            print(f"[INFO] Topic creation status: {e}")

        # Final verification
        cluster_metadata = await admin_client._client.fetch_all_metadata()
        final_topics = [t for t in cluster_metadata.topics() if not t.startswith("_")]
        print("\n==================================================================")
        print(f"[VERIFIED] Active Application Topics in Aiven: {final_topics}")
        print("==================================================================")

    finally:
        await admin_client.close()
        if temp_ca_path and os.path.exists(temp_ca_path):
            try:
                os.unlink(temp_ca_path)
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(init_kafka_topics())
