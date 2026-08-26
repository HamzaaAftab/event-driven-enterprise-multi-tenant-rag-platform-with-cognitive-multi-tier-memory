"""
Base Resilient Kafka Worker Framework for Async Event Consumption.
Provides manual offset commit, error retry, graceful shutdown, and Dead-Letter Queue (DLQ) routing.
"""

import abc
import asyncio
import json
import logging
import signal
import traceback
from typing import Any, Dict, Optional
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.core.kafka_producer import kafka_producer
from app.core.kafka_ssl import get_kafka_ssl_context
from app.schemas.events import DLQEvent

logger = logging.getLogger("base_worker")


class BaseKafkaWorker(abc.ABC):
    """
    Abstract Base Class for distributed Kafka event workers.
    Subclasses implement `process_message` to execute specific business tasks
    (e.g., LlamaParse document ingestion, Memory consolidation).
    """

    def __init__(
        self,
        topic: str,
        group_id: str,
        max_retries: int = 3,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self.topic = topic
        self.group_id = group_id
        self.max_retries = max_retries
        self.auto_offset_reset = auto_offset_reset
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._is_running = False

    @abc.abstractmethod
    async def process_message(self, payload: Dict[str, Any], key: Optional[str]) -> None:
        """
        Subclasses must implement this method to execute domain logic.
        Must raise an exception if processing fails so retries/DLQ can trigger.
        """
        pass

    async def _route_to_dlq(
        self,
        failed_payload: Dict[str, Any],
        error_message: str,
        stack_trace: str,
    ) -> None:
        """Publishes unrecoverable failed messages to the Dead-Letter Queue."""
        dlq_event = DLQEvent(
            original_topic=self.topic,
            failed_event=failed_payload,
            error_message=error_message,
            stack_trace=stack_trace,
        )
        try:
            logger.warning("[DLQ ROUTING] Routing failed message from '%s' to '%s'", self.topic, settings.KAFKA_TOPIC_DLQ)
            await kafka_producer.publish_event(
                topic=settings.KAFKA_TOPIC_DLQ,
                event=dlq_event,
                key=self.group_id,
            )
        except Exception as dlq_err:
            logger.error("[DLQ CRITICAL] Could not publish to DLQ topic: %s", dlq_err)

    async def start(self) -> None:
        """Starts the consumer loop with graceful signal handling."""
        ssl_ctx = get_kafka_ssl_context()
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            security_protocol=settings.KAFKA_SECURITY_PROTOCOL,
            sasl_mechanism=settings.KAFKA_SASL_MECHANISM,
            sasl_plain_username=settings.KAFKA_SASL_USERNAME,
            sasl_plain_password=settings.KAFKA_SASL_PASSWORD,
            ssl_context=ssl_ctx,
            enable_auto_commit=False,  # Manual commit ensures At-Least-Once Delivery
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
            retry_backoff_ms=1000,
        )

        logger.info("[WORKER START] Starting consumer for topic '%s' (Group: '%s')...", self.topic, self.group_id)
        await self._consumer.start()
        self._is_running = True
        logger.info("[WORKER READY] Listening for events on topic '%s'...", self.topic)

        try:
            async for msg in self._consumer:
                if not self._is_running:
                    break

                key = msg.key
                payload = msg.value
                logger.info("[EVENT RECEIVED] Topic: %s, Partition: %d, Offset: %d, Key: %s", msg.topic, msg.partition, msg.offset, key)

                # Process with retries
                success = False
                last_error = ""
                last_trace = ""

                for attempt in range(1, self.max_retries + 1):
                    try:
                        await self.process_message(payload, key)
                        success = True
                        break
                    except Exception as err:
                        last_error = str(err)
                        last_trace = traceback.format_exc()
                        logger.warning(
                            "[RETRY %d/%d] Error processing event on topic '%s': %s",
                            attempt,
                            self.max_retries,
                            self.topic,
                            err,
                        )
                        if attempt < self.max_retries:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff

                if success:
                    # Manually commit offset on success
                    await self._consumer.commit()
                    logger.debug("[COMMITTED] Offset %d committed on topic '%s'", msg.offset, msg.topic)
                else:
                    # Route to DLQ on permanent failure and commit so consumer is not blocked
                    await self._route_to_dlq(payload, last_error, last_trace)
                    await self._consumer.commit()

        except asyncio.CancelledError:
            logger.info("[WORKER CANCELLED] Worker loop cancelled.")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully stops the consumer."""
        self._is_running = False
        if self._consumer is not None:
            logger.info("[WORKER STOPPING] Stopping consumer for topic '%s'...", self.topic)
            await self._consumer.stop()
            self._consumer = None
            logger.info("[WORKER STOPPED] Consumer stopped successfully.")
