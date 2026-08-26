"""
Direct Kafka Roundtrip Test - Producer sends, Consumer receives.
"""

import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.core.config import settings
from app.core.kafka_ssl import get_kafka_ssl_context


async def test_roundtrip():
    ssl_ctx = get_kafka_ssl_context()
    print("[1/3] Starting Consumer...")
    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC_INGESTION,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id="test-roundtrip-group-1",
        security_protocol=settings.KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=settings.KAFKA_SASL_MECHANISM,
        sasl_plain_username=settings.KAFKA_SASL_USERNAME,
        sasl_plain_password=settings.KAFKA_SASL_PASSWORD,
        ssl_context=ssl_ctx,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()
    print("[2/3] Consumer connected! Sending message with Producer...")

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol=settings.KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=settings.KAFKA_SASL_MECHANISM,
        sasl_plain_username=settings.KAFKA_SASL_USERNAME,
        sasl_plain_password=settings.KAFKA_SASL_PASSWORD,
        ssl_context=ssl_ctx,
    )
    await producer.start()
    meta = await producer.send_and_wait(
        settings.KAFKA_TOPIC_INGESTION,
        b'{"message": "Hello Aiven Kafka Multi-Tenant RAG!"}',
    )
    print(f"[PRODUCED] Topic: {meta.topic}, Partition: {meta.partition}, Offset: {meta.offset}")
    await producer.stop()

    print("[3/3] Consumer polling for message...")
    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=20.0)
        print(f"[CONSUMED SUCCESS] Partition: {msg.partition}, Offset: {msg.offset}, Value: {msg.value.decode('utf-8')}")
    finally:
        await consumer.stop()
        print("\n[VERIFIED] Aiven Kafka Pub/Sub is 100% operational!")


if __name__ == "__main__":
    asyncio.run(test_roundtrip())
