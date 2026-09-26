# Distributed Event-Driven FinTech Ledger System

An asynchronous, double-entry ledger engine built with **FastAPI**, **PostgreSQL**, **Redis**, and **Google Cloud Pub/Sub**.

The system decouples transaction ingestion from financial settlement: the API validates a payment and responds immediately (`202 Accepted`), while a background worker performs the actual balance updates asynchronously, guarding against duplicate processing and lock-order deadlocks along the way.

---

## 🏗️ System Architecture

```text
[Client / Frontend]
        │
        ▼ (POST /charge)
┌────────────────────────────────────────────────────────┐
│ FastAPI Gateway (Producer)                              │
│  ├─ 1. Validate payload (Pydantic)                       │
│  ├─ 2. Check idempotency key (Redis, 24h TTL)             │
│  ├─ 3. Generate transaction_id (UUID4)                    │
│  └─ 4. Publish event to GCP Pub/Sub topic                 │
└────────────────────────────────────────────────────────┘
        │
        ├──────────────────────────────► Returns 202 ACCEPTED
        ▼ (Async event delivery)
┌────────────────────────────────────────────────────────┐
│ GCP Pub/Sub Event Bus                                    │
│ Topic: payment-processing-topic                           │
│ Subscription: ledger-worker-sub                            │
└────────────────────────────────────────────────────────┘
        │
        ▼ (Pull / event delivery — at-least-once)
┌────────────────────────────────────────────────────────┐
│ Python Settlement Worker (Consumer)                       │
│  ├─ 1. Ingest event payload                                │
│  ├─ 2. Reject redelivered/duplicate transactions           │
│  ├─ 3. Acquire row locks in a deterministic order (FOR UPDATE, ORDER BY id) │
│  ├─ 4. Verify account balances & business rules             │
│  ├─ 5. Write double-entry ledger records (PostgreSQL)        │
│  └─ 6. Issue ACK / NACK to Pub/Sub                          │
└────────────────────────────────────────────────────────┘
```

---

## Key Engineering Problems Solved

### 1. Concurrency Control & Deadlock Prevention

**The problem:** two transactions touching the same two accounts in opposite order (A pays B while B pays A, concurrently) can each lock one account and then block waiting for the other — a circular wait Postgres reports as `deadlock detected`.

**The solution:** the row-locking query (`SELECT ... FOR UPDATE`) is given an explicit `ORDER BY id`. Postgres acquires `FOR UPDATE` locks in the order rows are returned, so an `ORDER BY` on a stable column guarantees every transaction touching these two accounts locks them in the same sequence, regardless of which account is paying and which is receiving. That removes the circular wait structurally.

*(Note: sorting the account IDs in Python before the query does **not** achieve this — `WHERE id IN (...)` does not preserve or use the order of the list. The ordering has to happen in the SQL query itself, via `ORDER BY`, since that's what actually controls the order Postgres locks the returned rows in.)*

Verified with a threaded test (`api/app/test_deadlock.py`) that deterministically forces both scenarios: two connections locking the same two rows in opposite order (confirmed deadlock, SQLSTATE `40P01`), and in the same order (confirmed clean serialization, no deadlock).

### 2. Idempotency Under At-Least-Once Delivery

**The problem:** Pub/Sub guarantees *at-least-once* delivery, not exactly-once — the same message can be redelivered to the worker (e.g. a lost ack, or the worker taking too long to acknowledge). Without protection, a redelivered payment message would move money a second time.

**The solution — two layers:**
- **API boundary (Redis):** a fast idempotency-key check on `/charge` rejects an immediate duplicate client request with `409 Conflict`, before any Pub/Sub or database resources are touched.
- **Worker / database boundary:** the worker itself checks for an existing `Transaction` with the same `idempotency_key` before processing, and the column also carries a database-level `UNIQUE` constraint as a backstop for the case where two concurrent workers pass that check at nearly the same instant. Either path resolves to the same outcome — the message is acknowledged (removed from the queue) without settling the payment a second time.

Verified in `api/app/test_idempotency.py`: sends the same payment twice, asserts the second call is rejected as a duplicate, and confirms the account balances only moved once.

*(Known limitation: the Redis check at the API boundary is a check-then-set, not atomic — two near-simultaneous requests with the same key could both pass it. The worker/database layer is what actually guarantees correctness in that case; the Redis check is a fast-path optimization, not the source of truth.)*

### 3. Decoupled Ingestion

The API never writes to the database directly in the request path — it validates, publishes to Pub/Sub, and returns `202 Accepted`. Settlement happens asynchronously in the worker. This keeps the API responsive even if the database is briefly slow, at the cost of the client not knowing the payment's final outcome at request time (see Known Limitations — there is currently no endpoint to check a transaction's status after the fact).

### 4. Transaction ID Consistency

**The problem:** the API generates a `transaction_id` and returns it to the client immediately (before the worker has processed anything). That same ID needs to end up as the actual primary key of the row the worker eventually creates — otherwise the client is left holding a tracking number that never exists in the database.

**The solution:** the Pub/Sub message is built from a `PaymentTask` schema (client-facing `PaymentRequest` plus the generated `transaction_id`), and the worker explicitly passes that ID through as the database row's `id` rather than letting SQLAlchemy generate a new one by default.

Verified by asserting, in `test_idempotency.py`, that the ID returned from `execute_payment()` is exactly equal to the ID generated before the call — not just "no error was raised."

### 5. Worker ACK/NACK Handling

- **Duplicate delivery:** ACK — already resolved, don't reprocess (see #2 and #6 below).
- **Business rule failure** (e.g. insufficient funds): ACK — this is a legitimate rejection, retrying it would never succeed. Now also persisted as a `FAILED` transaction (see #6).
- **Unexpected/system error** (DB connection issue, etc.): NACK — Pub/Sub will redeliver later.

### 6. Failed Payments Are Recorded, Not Silently Dropped

**The problem:** a business-rule rejection (insufficient funds) used to just log a message and vanish — no row in the database, no audit trail. A rejected payment is a real event a fintech ledger needs to keep.

**The solution:** on a business-rule `ValueError`, the worker's DB layer rolls back the (already-undone) balance mutations, then — as a separate, subsequent unit of work — commits a `Transaction` row with `status=FAILED`, carrying the same `id` the client was already given. `DuplicateTransactionError` was extended to carry the existing record's `status`, so a redelivery of an already-failed payment is correctly reported as a duplicate-of-a-failure, not misreported as "already settled."

*(A subtlety worth naming: this introduces its own small race — two concurrent attempts at the same business-rule failure could both try to commit a `FAILED` row under the same `idempotency_key`. That commit is wrapped in its own nested `try/except IntegrityError`, specifically because a sibling `except` clause on the same `try` block cannot catch an exception raised from inside another `except` block — it has to be handled at the point it can actually occur.)*

Verified in `api/app/test_failed_transaction.py`: a solo insufficient-funds payment leaves a `FAILED` row with the correct id, a redelivery is recognized as a duplicate-of-a-failure, and a forced concurrent race (two threads, same idempotency_key, same failure) is confirmed to resolve cleanly — one `FAILED` row, one correctly-reported duplicate, no crash, no infinite retry.

### 7. Double-Entry Bookkeeping

Every payment creates a `Transaction` linked to `LedgerLine` rows recording the debit and credit sides. `Account.balance` is a stored column, updated alongside the ledger rows in the same atomic transaction — it is not currently derived by summing the ledger at read time. A reconciliation check (verifying stored balance matches the sum of ledger movements) is on the roadmap but not yet implemented.

---

## Tech Stack

- **Language:** Python 3.11+
- **API Framework:** FastAPI, Uvicorn, Pydantic
- **Persistence:** PostgreSQL 15, SQLAlchemy, Alembic
- **Cache:** Redis 7 (Alpine)
- **Message Broker:** Google Cloud Pub/Sub (local emulator for development)
- **Containerization:** Docker, Docker Compose

---

## Getting Started

### Prerequisites
- Docker Desktop (macOS, Windows, or Linux)
- `git` and `curl`

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/fintech-ledger-system.git
cd fintech-ledger-system
```

### 2. Boot the infrastructure

Spins up the API, worker, PostgreSQL, Redis, and the Pub/Sub emulator:

```bash
docker compose up --build -d
docker compose ps
```

### 3. Run database migrations

```bash
docker compose exec api alembic upgrade head
```

### 4. Seed test accounts

```bash
docker compose exec db psql -U postgres -d ledger_db -c "
INSERT INTO accounts (id, name, type, balance)
VALUES
('11111111-1111-1111-1111-111111111111', 'Alice', 'USER', 10000),
('22222222-2222-2222-2222-222222222222', 'Bob Coffee', 'MERCHANT', 0);
"
```

---

## Testing the Pipeline

### 1. Submit a payment

```bash
curl -X POST "http://localhost:8000/charge" \
  -H "Content-Type: application/json" \
  -d '{
        "idempotency_key": "txn_live_test_001",
        "user_account_id": "11111111-1111-1111-1111-111111111111",
        "merchant_account_id": "22222222-2222-2222-2222-222222222222",
        "amount_cents": 1500,
        "description": "Espresso and Croissant"
      }'
```

Expected response (`202 Accepted`):

```json
{
  "transaction_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "status": "QUEUED",
  "message": "Payment accepted for background processing."
}
```

### 2. Observe background settlement

```bash
docker compose logs worker --tail 20
```

Expected output (the message ID Pub/Sub assigns will differ from this example):

```
[WORKER] Received Ticket: 2810521427192384
[WORKER] Processing Payment for 1500 cents...
[WORKER] SUCCESS! Ledger updated. Transaction DB ID: 9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d
```

Note that the `Transaction DB ID` here matches the `transaction_id` returned by step 1 — that consistency is exactly what's verified in `api/app/test_idempotency.py`.

### 3. Verify account balances

```bash
docker compose exec db psql -U postgres -d ledger_db -c "SELECT id, name, balance FROM accounts;"
```

### 4. Run the verification scripts directly

```bash
docker compose run --rm api python -m app.test_idempotency
docker compose run --rm api python -m app.test_deadlock
docker compose run --rm api python -m app.test_failed_transaction
```

---

## ☁️ Deploying to GCP (No Paid Services Required)

This runs entirely on free tiers:

| Component | Local (docker compose) | GCP deployment |
|---|---|---|
| API | `api` container | Cloud Run (scale-to-zero, generous always-free tier) |
| Worker | `worker` container | Cloud Run (push subscription target) |
| Message broker | Pub/Sub emulator | Pub/Sub (10 GB/month free) |
| Database | `db` container (Postgres) | [Neon](https://neon.tech) or [Supabase](https://supabase.com) free tier — Cloud SQL is not free |
| Cache | `redis` container | [Upstash](https://upstash.com) free tier — Memorystore is not free |
| Container registry | — | Artifact Registry (0.5 GB free) |

*(This section describes the intended deployment path; it has not yet been executed end-to-end from this repo — see Known Limitations.)*

---

## Known Limitations / Roadmap

Being upfront about what's not done yet:

- No `GET /transactions/{id}` or `GET /accounts/{id}` — a client currently has no way to check a payment's status after the initial `202` response.
- No `/health` endpoint.
- No authentication on the API.
- No automated test suite (pytest) yet — verification currently relies on the manual scripts described above (`test_idempotency.py`, `test_deadlock.py`, `test_failed_transaction.py`).
- No reconciliation check between stored `Account.balance` and the sum of ledger movements.
- No currency validation — a transfer between accounts with different currencies is not currently rejected.
- GCP deployment (Cloud Run + Neon + Upstash) is designed but not yet deployed from this repo.
- No CI pipeline.

This project is a personal, in-progress learning exercise mirroring architecture patterns from professional backend work — not a finished product. Bugs found and fixed so far, with the reasoning behind each, are in the commit history.
