"""
Async Kafka Producer Service for Aiven Apache Kafka.
Handles structured event publishing, JSON serialization, and partition keying.
"""

import json
import logging
import uuid
from typing import Any, Dict, Optional, Union
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel
from app.core.config import settings
from app.core.kafka_ssl import get_kafka_ssl_context

logger = logging.getLogger("kafka_producer")


class KafkaProducerService:
    """Singleton wrapper around AIOKafkaProducer with partition keying and serialization."""

    def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        """Starts the Kafka producer connection pool."""
        if self._producer is not None:
            return

        ssl_ctx = get_kafka_ssl_context()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            security_protocol=settings.KAFKA_SECURITY_PROTOCOL,
            sasl_mechanism=settings.KAFKA_SASL_MECHANISM,
            sasl_plain_username=settings.KAFKA_SASL_USERNAME,
            sasl_plain_password=settings.KAFKA_SASL_PASSWORD,
            ssl_context=ssl_ctx,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
            retry_backoff_ms=500,
            request_timeout_ms=15000,
        )
        logger.info("[KAFKA PRODUCER] Connecting to Aiven Kafka broker: %s", settings.KAFKA_BOOTSTRAP_SERVERS)
        await self._producer.start()
        logger.info("[KAFKA PRODUCER] Successfully connected to Kafka!")

    async def stop(self) -> None:
        """Flushes buffered messages and closes the Kafka producer."""
        if self._producer is not None:
            logger.info("[KAFKA PRODUCER] Closing Kafka producer...")
            await self._producer.stop()
            self._producer = None
            logger.info("[KAFKA PRODUCER] Kafka producer closed.")

    async def publish_event(
        self,
        topic: str,
        event: Union[BaseModel, Dict[str, Any]],
        key: Optional[Union[str, uuid.UUID]] = None,
    ) -> Dict[str, Any]:
        """
        Publishes a message/event to a specified Kafka topic.
        - `event`: Pydantic BaseModel instance or dict payload.
        - `key`: Partition key (e.g. tenant_id or user_id) to guarantee partition ordering.
        Returns metadata dict containing topic, partition, and offset.
        """
        if self._producer is None:
            await self.start()

        # Convert Pydantic models to serializable dict
        if isinstance(event, BaseModel):
            payload = json.loads(event.model_dump_json())
        else:
            payload = event

        partition_key = str(key) if key is not None else None

        try:
            record_metadata = await self._producer.send_and_wait(
                topic=topic,
                value=payload,
                key=partition_key,
            )
            result = {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
            }
            logger.debug("[KAFKA SENT] Topic: %s, Partition: %d, Offset: %d", result["topic"], result["partition"], result["offset"])
            return result
        except Exception as e:
            logger.error("[KAFKA ERROR] Failed to send event to %s: %s", topic, e)
            raise


kafka_producer = KafkaProducerService()


async def get_kafka_producer() -> KafkaProducerService:
    """Helper dependency to retrieve started producer instance."""
    if kafka_producer._producer is None:
        await kafka_producer.start()
    return kafka_producer
11