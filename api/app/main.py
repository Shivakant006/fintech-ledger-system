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
    # 1. IDEMPOTENCY CHECK (Still fast, still necessary)
    if cache.exists(f"idempotency:{request.idempotency_key}"):
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
        
        # 5. LOCK THE CACHE
        # Only lock Redis AFTER Pub/Sub successfully receives the message.
        cache.setex(f"idempotency:{request.idempotency_key}", 86400, str(transaction_id))

        return schemas.PaymentResponse(
            transaction_id=transaction_id,
            status="QUEUED",
            message="Payment accepted for background processing."
        )
        
    except Exception as e:
        # If the Pub/Sub system is completely down, fail the request safely.
        raise HTTPException(status_code=500, detail="Failed to queue payment.")