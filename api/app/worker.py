import os
import json
import time
from google.cloud import pubsub_v1
from pydantic import ValidationError

# Reuse our existing database and logic layers!
from . import crud, schemas, models
from .crud import DuplicateTransactionError
from .database import SessionLocal

PROJECT_ID = os.getenv("PROJECT_ID", "local-project")
SUBSCRIPTION_ID = "ledger-worker-sub"

def process_message(message: pubsub_v1.subscriber.message.Message):
    """This is the callback function. It runs every time a new message arrives."""
    print(f"\n[WORKER] Received Ticket: {message.message_id}")
    
    try:
        # 1. Parse the incoming JSON bytes back into a Python dictionary
        payload = json.loads(message.data.decode("utf-8"))
        print(f"[WORKER] Processing Payment for {payload.get('amount_cents')} cents...")
        
        # 2. Validate it using PaymentTask — the internal schema that
        # includes transaction_id (PaymentRequest is client-facing only
        # and deliberately has no transaction_id field, since the client
        # never sends one).
        payment_data = schemas.PaymentTask(**payload)
        
        # 3. Open a database session
        db = SessionLocal()
        try:
            # 4. EXECUTE THE HEAVY MATH (Row Locks, Double-Entry)
            # We reuse the exact same crud.py function from Phase 3!
            txn = crud.execute_payment(db=db, payment_data=payment_data)
            print(f"[WORKER] SUCCESS! Ledger updated. Transaction DB ID: {txn.id}")
            
            # 5. ACKNOWLEDGE (ACK)
            # This tells Pub/Sub: "I successfully processed this. Delete it from the queue."
            message.ack()
            
        except DuplicateTransactionError as e:
            # This exact idempotency_key was already resolved once —
            # either COMPLETED (a prior success, or a concurrent worker won
            # a race) or FAILED (a prior business-rule rejection). Either
            # way we don't touch account balances again — replaying an
            # idempotency key never re-runs the business logic, it reports
            # what already happened. We DO still distinguish the two in
            # the log, since "already settled" would be misleading for a
            # duplicate of a FAILED payment.
            if e.status == models.TransactionStatus.FAILED:
                print(f"[WORKER] DUPLICATE of a PREVIOUSLY FAILED payment "
                      f"(not retried automatically — client would need a "
                      f"new idempotency_key to retry): {e}")
            else:
                print(f"[WORKER] DUPLICATE (already settled): {e}")
            message.ack()

        except ValueError as e:
            # Business logic failure (e.g., Insufficient Funds)
            print(f"[WORKER] REJECTED (Business Rule): {e}")
            # We ACK it so it deletes from the queue (we don't want to retry a failed payment)
            message.ack() 
        except Exception as e:
            # Database crash or Row Lock timeout
            print(f"[WORKER] SYSTEM ERROR: {e}")
            # We NACK (Negative Acknowledge) it. Pub/Sub will retry this message later!
            message.nack()
        finally:
            db.close()
            
    except ValidationError as e:
        print(f"[WORKER] Corrupt Message Payload: {e}")
        message.ack() # Delete corrupt messages so they don't clog the queue

def start_worker():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    print(f"[*] Starting Settlement Worker. Listening to {subscription_path}...")
    
    # Attach our callback function to the subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_message)
    
    try:
        # Keep the main thread alive so the background listener keeps running
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    # Give the database and emulator a few seconds to boot up before listening
    time.sleep(5) 
    start_worker()