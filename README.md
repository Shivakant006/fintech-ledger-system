# Distributed Event-Driven FinTech Ledger System

An asynchronous, ACID-compliant double-entry ledger engine built with **FastAPI**, **PostgreSQL**, **Redis**, and **Google Cloud Pub/Sub**. 

The system decouples transaction ingestion from financial settlement, providing sub-millisecond API response times while guaranteeing zero data loss, strict idempotency, and deadlock-free concurrent execution.

---

## 🏗️ System Architecture

```text
[Client / Frontend]
        │
        ▼ (POST /charge)
┌────────────────────────────────────────────────────────┐
│ FastAPI Gateway (Producer)                             │
│  ├─ 1. Validate payload (Pydantic)                     │
│  ├─ 2. Check & Set Idempotency Key (Redis, 24h TTL)    │
│  ├─ 3. Generate Client Transaction ID (UUID4)          │
│  └─ 4. Publish Event to GCP Pub/Sub Topic              │
└────────────────────────────────────────────────────────┘
        │
        ├──────────────────────────────► Returns 202 ACCEPTED
        ▼ (Async Event Ingestion)
┌────────────────────────────────────────────────────────┐
│ GCP Pub/Sub Event Bus                                  │
│ Topic: payment-processing-topic                        │
│ Subscription: ledger-worker-sub                        │
└────────────────────────────────────────────────────────┘
        │
        ▼ (Pull / Event Delivery)
┌────────────────────────────────────────────────────────┐
│ Python Settlement Worker (Consumer)                    │
│  ├─ 1. Ingest event payload                            │
│  ├─ 2. Acquire Alphabetical Row Locks (FOR UPDATE)     │
│  ├─ 3. Verify Account Balances & Business Rules        │
│  ├─ 4. Write Double-Entry Ledger Records (PostgreSQL)  │
│  └─ 5. Issue ACK / NACK to Pub/Sub                     │
└────────────────────────────────────────────────────────┘
```text
[Client / Frontend]
        │
        ▼ (POST /charge)
┌────────────────────────────────────────────────────────┐
│ FastAPI Gateway (Producer)                             │
│  ├─ 1. Validate payload (Pydantic)                     │
│  ├─ 2. Check & Set Idempotency Key (Redis, 24h TTL)    │
│  ├─ 3. Generate Client Transaction ID (UUID4)          │
│  └─ 4. Publish Event to GCP Pub/Sub Topic              │
└────────────────────────────────────────────────────────┘
        │
        ├──────────────────────────────► Returns 202 ACCEPTED
        ▼ (Async Event Ingestion)
┌────────────────────────────────────────────────────────┐
│ GCP Pub/Sub Event Bus                                  │
│ Topic: payment-processing-topic                        │
│ Subscription: ledger-worker-sub                        │
└────────────────────────────────────────────────────────┘
        │
        ▼ (Pull / Event Delivery)
┌────────────────────────────────────────────────────────┐
│ Python Settlement Worker (Consumer)                    │
│  ├─ 1. Ingest event payload                            │
│  ├─ 2. Acquire Alphabetical Row Locks (FOR UPDATE)     │
│  ├─ 3. Verify Account Balances & Business Rules        │
│  ├─ 4. Write Double-Entry Ledger Records (PostgreSQL)  │
│  └─ 5. Issue ACK / NACK to Pub/Sub                     │
└────────────────────────────────────────────────────────┘
Key Engineering Problems Solved
1. Concurrency Control & Deadlock Elimination
        The Problem: In high-throughput payment systems, concurrent transactions involving overlapping accounts (e.g., Alice sends to Bob while Bob simultaneously sends to Alice) cause circular lock waits in PostgreSQL, triggering deadlocks.

        The Solution: Implemented deterministic, alphabetical row-level locking via SQLAlchemy (SELECT ... FOR UPDATE). By sorting account UUIDs lexicographically before querying the database, transactions acquire locks in an identical global order, eliminating circular wait conditions mathematically.

2. Distributed Idempotency
        The Problem: Network timeouts or aggressive client retries can lead to duplicate payments.

        The Solution: Integrated Redis-backed idempotency guards at the API boundary. Requests carrying a previously processed idempotency_key are rejected with 409 CONFLICT before any database or messaging resources are consumed.

3. Decoupled Ingestion & Backpressure Resilience
        The Problem: Direct database writes in the HTTP request path introduce latency bottlenecks and risk dropping payments during database failovers or traffic spikes.

        The Solution: The API acts strictly as an event producer, pushing validated payment payloads to a GCP Pub/Sub message broker and returning 202 ACCEPTED. Financial settlement occurs asynchronously via dedicated background workers.

4. Resilient Worker Processing (ACK/NACK Pattern)
        The Problem: Worker crashes or intermittent database disconnects can cause silent data loss or poison-pill infinite loops.

        The Solution:

                System Errors (Database downtime, lock timeout): The worker issues a NACK (Negative Acknowledgment), leaving the message in Pub/Sub for automated exponential-backoff retry.

                Business Rule Failures (Insufficient funds, missing account): The worker records the business failure and issues an ACK to cleanly remove the poison pill from the queue.

5. Strict Double-Entry Bookkeeping
        The Problem: Single-column balance increments (balance = balance - amount) leave no audit trail and risk balance drift.

        The Solution: Every financial event creates an immutable Transaction record linked to two equal and opposite LedgerEntry records (one DEBIT, one CREDIT). Account balances are strictly derived from these ledger movements within an atomic database transaction.

Tech Stack
        Language: Python 3.11+

        API Framework: FastAPI, Uvicorn, Pydantic

        Persistence: PostgreSQL 15, SQLAlchemy, Alembic

        In-Memory Cache: Redis 7 (Alpine)

        Message Broker: Google Cloud Pub/Sub (Local Emulator)

        Containerization: Docker, Docker Compose

Getting Started
        Prerequisites
                Docker Desktop (macOS, Windows, or Linux)

                git and curl

1. Clone the Repository
        #Bash
        git clone [https://github.com/YOUR_USERNAME/fintech-ledger-system.git](https://github.com/YOUR_USERNAME/fintech-ledger-system.git)
        cd fintech-ledger-system
2. Boot the Infrastructure
        Run the following command to spin up the API, Worker, PostgreSQL, Redis, Pub/Sub Emulator, and the initialization script:
                #Bash
                docker compose up --build -d
        Verify that all services are running:
                #Bash
                docker compose ps
3. Run Database Migrations
        Apply Alembic migrations inside the API container to construct the schema:
        #Bash
        docker compose exec api alembic upgrade head
4. Seed Test Accounts
        Create two baseline accounts (Alice with 10,000 cents / $100.00, Bob with 0 cents):
        #Bash
        docker compose exec db psql -U postgres -d ledger_db -c "
        INSERT INTO accounts (id, name, type, balance) 
        VALUES 
        ('11111111-1111-1111-1111-111111111111', 'Alice', 'USER', 10000), 
        ('22222222-2222-2222-2222-222222222222', 'Bob Coffee', 'MERCHANT', 0);
        "
Testing the Pipeline
1. Submit a Payment
        Dispatch an asynchronous payment request to the API:
                #Bash
                curl -X POST "http://localhost:8000/charge" \
                -H "Content-Type: application/json" \
                -d '{
                        "idempotency_key": "txn_live_test_001",
                        "user_account_id": "11111111-1111-1111-1111-111111111111",
                        "merchant_account_id": "22222222-2222-2222-2222-222222222222",
                        "amount_cents": 1500,
                        "description": "Espresso and Croissant"
                        }'
        Expected Response (202 Accepted):

        #JSON
                {
                "transaction_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
                "status": "QUEUED",
                "message": "Payment accepted for background processing."
                }
2. Observe Background Settlement
Check the worker container logs to verify message ingestion and ledger updates:

        #Bash
                docker compose logs worker --tail 20

Expected Output:

        Plaintext
        [WORKER] Received Ticket: 1
        [WORKER] Processing Payment for 1500 cents...
        [WORKER] SUCCESS! Ledger updated. Transaction DB ID: 9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d
3. Verify Account Balances
Inspect the database to ensure Alice was debited and Bob was credited:

        #Bash
                docker compose exec db psql -U postgres -d ledger_db -c "SELECT id, name, balance FROM accounts;"
☁️ Production Cloud Architecture (GCP Reference)
In an enterprise Google Cloud environment, this architecture scales serverlessly with zero code modification:

API: Google Cloud Run (Public HTTP Service, auto-scales based on incoming web traffic).

Broker: Google Cloud Pub/Sub (Globally distributed, persistent message log).

Worker: Google Cloud Run (Private Push-Subscription target, scales to zero when no transactions are pending).

Storage: Cloud SQL for PostgreSQL (Multi-AZ High Availability, automated point-in-time recovery).

Cache: Cloud Memorystore for Redis (In-memory cluster with VPC peering).
