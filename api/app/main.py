from fastapi import FastAPI, Depends, HTTPException, status
import redis
import json
import os
import uuid
from google.cloud import pubsub_v1

from . import schemas
from .redis_client import get_redis

app = FastAPI(title="FinTech Ledger API (Decoupled)")

# ---------------------------------------------------------
# PUB/SUB CONFIGURATION
# ---------------------------------------------------------
PROJECT_ID = os.getenv("PROJECT_ID", "local-project")
TOPIC_ID = "payment-processing-topic"

# Create the Publisher Client. 
# Because PUBSUB_EMULATOR_HOST is set in docker-compose, this automatically routes locally.
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# Notice we changed the status code to 202 ACCEPTED.
# In distributed systems, 202 means: "Valid request, accepted for processing, but not finished."
@app.post("/charge", response_model=schemas.PaymentResponse, status_code=status.HTTP_202_ACCEPTED)
def process_payment(
    request: schemas.PaymentRequest, 
    cache: redis.Redis = Depends(get_redis)
):
    # 1. IDEMPOTENCY CHECK — CLAIM THE KEY ATOMICALLY
    # The old version did cache.exists(...) then, much later, cache.setex(...) —
    # two separate round-trips to Redis, with transaction_id generation and a
    # full Pub/Sub publish call happening in between. Two concurrent requests
    # with the same idempotency_key could BOTH pass the exists() check before
    # either one reached setex(), both publish their own message with their
    # own separate transaction_id, and the loser's transaction_id would never
    # exist in the database once the worker's own duplicate check rejected it.
    #
    # set(..., nx=True) closes that gap: Redis executes "check and claim" as
    # ONE indivisible operation. Only one concurrent caller can ever succeed;
    # everyone else is told the key is taken, immediately, before generating
    # a transaction_id or touching Pub/Sub at all.
    lock_key = f"idempotency:{request.idempotency_key}"
    claimed = cache.set(lock_key, "processing", nx=True, ex=86400)
    if not claimed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Transaction already processed (Idempotency Key collision)."
        )

    # 2. GENERATE THE ID NOW (The Microservice Way)
    # Instead of letting the database generate the ID later, the API generates it now.
    # This allows us to give the user a tracking number instantly.
    transaction_id = uuid.uuid4()

    # 3. CONSTRUCT THE TICKET (The Pub/Sub Message payload)
    # Build a PaymentTask — everything the client sent in `request`, PLUS
    # the transaction_id we just generated. Building it as a real schema
    # object (instead of a hand-typed dict) means if PaymentRequest ever
    # gains a new field, it's automatically included here too — nothing
    # to remember to update in two places.
    task = schemas.PaymentTask(
        **request.model_dump(),
        transaction_id=transaction_id,
    )

    # Pub/Sub requires messages to be bytes, not dictionaries.
    # mode="json" makes sure UUID objects are serialized as strings.
    message_bytes = json.dumps(task.model_dump(mode="json")).encode("utf-8")

    # 4. PUBLISH TO THE RAIL
    try:
        # This sends the message to Pub/Sub and waits for Google to say "Got it."
        future = publisher.publish(topic_path, data=message_bytes)
        future.result()

        # We already claimed the lock in step 1 (with a placeholder value).
        # Now that publish succeeded, overwrite it with the real
        # transaction_id, keeping the same 24h expiry, so a status lookup
        # against this key later reflects the actual transaction.
        cache.setex(lock_key, 86400, str(transaction_id))

        return schemas.PaymentResponse(
            transaction_id=transaction_id,
            status="QUEUED",
            message="Payment accepted for background processing."
        )
        
    except Exception as e:
        # RESERVE, THEN RELEASE ON FAILURE.
        # We claimed the idempotency key in step 1, optimistically, before
        # we knew whether Pub/Sub would actually accept the message. If
        # publish failed, this request never actually got queued — so the
        # client retrying with the SAME idempotency_key is a legitimate
        # retry, not a duplicate. If we left the lock in place for its full
        # 24h TTL, that legitimate retry would be wrongly rejected as a
        # collision. So we must release the lock here, BEFORE raising the
        # error back to the client, so their retry is free to succeed.
        cache.delete(lock_key)
        raise HTTPException(status_code=500, detail="Failed to queue payment.")