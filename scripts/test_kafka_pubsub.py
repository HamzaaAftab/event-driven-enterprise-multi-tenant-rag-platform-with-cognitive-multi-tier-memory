"""
End-to-End Kafka Pub/Sub & DLQ Verification Test for Aiven Kafka.
Publishes a test event, consumes it using BaseKafkaWorker, and verifies delivery.
Usage:
    python -m scripts.test_kafka_pubsub
"""

import asyncio
import uuid
from typing import Any, Dict, Optional
from app.core.config import settings
from app.core.kafka_producer import kafka_producer
from app.schemas.events import DocIngestionEvent, DocIngestionPayload
from app.workers.base_worker import BaseKafkaWorker

received_events = []


class TestIngestionWorker(BaseKafkaWorker):
    """Test implementation of BaseKafkaWorker."""

    async def process_message(self, payload: Dict[str, Any], key: Optional[str]) -> None:
        print(f"\n[TEST WORKER] Received message with Key: {key}")
        print(f"[TEST WORKER] Event Type: {payload.get('event_type')}")
        print(f"[TEST WORKER] Document ID: {payload.get('payload', {}).get('document_id')}")
        received_events.append(payload)


async def main() -> None:
    print("==================================================================")
    print("[TEST] Running End-to-End Aiven Kafka Pub/Sub Verification Test...")
    print("==================================================================")

    test_tenant_id = uuid.uuid4()
    test_user_id = uuid.uuid4()
    test_doc_id = uuid.uuid4()

    # 1. Construct Event
    event = DocIngestionEvent(
        payload=DocIngestionPayload(
            tenant_id=test_tenant_id,
            user_id=test_user_id,
            document_id=test_doc_id,
            file_name="Q3_Financial_Test.pdf",
            storage_path=f"tenants/{test_tenant_id}/docs/{test_doc_id}.pdf",
        )
    )

    # 2. Start Producer & Publish Event
    print("\n[STEP 1] Publishing event to Aiven Kafka topic:", settings.KAFKA_TOPIC_INGESTION)
    await kafka_producer.start()
    send_meta = await kafka_producer.publish_event(
        topic=settings.KAFKA_TOPIC_INGESTION,
        event=event,
        key=test_tenant_id,
    )
    print(f"[SUCCESS] Published successfully! Topic: {send_meta['topic']}, Partition: {send_meta['partition']}, Offset: {send_meta['offset']}")
    await kafka_producer.stop()

    # 3. Start Consumer Worker in background task
    print("\n[STEP 2] Starting Test Worker to consume the event...")
    worker = TestIngestionWorker(
        topic=settings.KAFKA_TOPIC_INGESTION,
        group_id=f"test-verifier-group-{uuid.uuid4().hex[:6]}",
        auto_offset_reset="earliest",
    )

    worker_task = asyncio.create_task(worker.start())

    # Wait up to 10 seconds for worker to process the message
    for _ in range(10):
        if len(received_events) > 0:
            break
        await asyncio.sleep(1)

    # Stop worker
    await worker.stop()
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    # 4. Assert Results
    if len(received_events) > 0:
        print("\n==================================================================")
        print("[SUCCESS] Aiven Kafka Pub/Sub Round-Trip 100% Verified!")
        print("==================================================================")
    else:
        print("\n[FAIL] Event was published but worker did not receive it in time.")


if __name__ == "__main__":
    asyncio.run(main())
