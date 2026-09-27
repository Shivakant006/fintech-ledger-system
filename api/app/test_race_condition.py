"""
Manual verification script for the Redis check-then-set race at the
/charge API boundary.

Unlike test_idempotency.py, test_deadlock.py, and test_failed_transaction.py
— which all call crud.execute_payment() directly, inside the same process —
this script fires TWO REAL, CONCURRENT HTTP REQUESTS at the actual running
API container. That's deliberate: this bug lives in main.py's endpoint
function itself (FastAPI routing, the Depends(get_redis) dependency, the
real Redis round-trip, the real Pub/Sub publish call). A test that calls
Python functions directly, in-process, wouldn't exercise any of that — it
would only prove our specific lines of logic work in isolation, not that
the real request path is race-free.

IMPORTANT — how to run this:
This must run INSIDE the already-running `api` container (sharing its
network namespace, so "localhost:8000" reaches the live uvicorn process
sitting right next to it) — NOT via `docker compose run --rm`, which
spins up a SEPARATE, unrelated container:

    docker compose up -d          # make sure everything is running first
    docker compose exec api python -m app.test_race_condition

What it does:
  1. Creates two fresh accounts (same pattern as the other test scripts).
  2. Fires two HTTP POST /charge requests at the SAME instant (synchronized
     with a threading.Barrier), using the SAME idempotency_key but each
     built and sent independently — exactly like two real, unrelated
     concurrent clients would.
  3. Asserts exactly one request got 202 Accepted and the other got 409
     Conflict — if BOTH got 202, the race is not fixed.
  4. Polls the database briefly (the worker processes asynchronously) and
     asserts exactly ONE Transaction row exists for that idempotency_key,
     and that its id matches the transaction_id from the 202 response —
     proving the "winner" client isn't left holding a phantom id either.
"""
import json
import threading
import time
import urllib.request
import urllib.error
import uuid

from . import models
from .database import SessionLocal, engine, Base

API_URL = "http://localhost:8000/charge"


def _setup_accounts():
    db = SessionLocal()
    try:
        run_id = uuid.uuid4().hex[:8]
        user = models.Account(name=f"Race Test User {run_id}",
                               type=models.AccountType.USER, balance=10_000)
        merchant = models.Account(name=f"Race Test Merchant {run_id}",
                                   type=models.AccountType.MERCHANT, balance=0)
        db.add_all([user, merchant])
        db.commit()
        db.refresh(user)
        db.refresh(merchant)
        return user.id, merchant.id
    finally:
        db.close()


def _fire_request(payload, barrier, results, idx):
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        API_URL, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    barrier.wait()  # both threads send their request at the same instant
    try:
        with urllib.request.urlopen(req) as resp:
            results[idx] = (resp.status, json.loads(resp.read()))
    except urllib.error.HTTPError as e:
        results[idx] = (e.code, json.loads(e.read()))


def main():
    Base.metadata.create_all(bind=engine)
    user_id, merchant_id = _setup_accounts()
    print(f"[SETUP] user={user_id} merchant={merchant_id}")

    idempotency_key = f"test-race-http-{uuid.uuid4().hex[:8]}"
    payload = {
        "idempotency_key": idempotency_key,
        "user_account_id": str(user_id),
        "merchant_account_id": str(merchant_id),
        "amount_cents": 1500,
        "description": "HTTP-level race test",
    }

    print(f"\n[FIRE] Sending two concurrent /charge requests, same "
          f"idempotency_key={idempotency_key}...")
    barrier = threading.Barrier(2)
    results = [None, None]
    t1 = threading.Thread(target=_fire_request, args=(payload, barrier, results, 0))
    t2 = threading.Thread(target=_fire_request, args=(payload, barrier, results, 1))
    t1.start(); t2.start()
    t1.join(timeout=10); t2.join(timeout=10)

    for i, r in enumerate(results):
        print(f"  request {i}: status={r[0] if r else 'NO RESPONSE'} body={r[1] if r else ''}")

    statuses = sorted(r[0] for r in results if r)
    if statuses != [202, 409]:
        print(f"\n[RESULT] FAIL — expected exactly one 202 and one 409, "
              f"got statuses={statuses}. If both are 202, the race is NOT fixed.")
        return

    winner = next(r for r in results if r[0] == 202)
    winning_transaction_id = winner[1]["transaction_id"]
    print(f"\n[VERIFY] Exactly one 202 (id={winning_transaction_id}) and one 409, as expected.")

    # The worker processes asynchronously — poll briefly rather than assume
    # it's instantaneous.
    db = SessionLocal()
    try:
        row = None
        for _ in range(20):
            row = db.query(models.Transaction).filter_by(idempotency_key=idempotency_key).first()
            if row:
                break
            time.sleep(0.5)

        if row is None:
            print("[VERIFY] FAIL — no Transaction row appeared within 10s. "
                  "(Is the worker container running? `docker compose ps`)")
            return

        count = db.query(models.Transaction).filter_by(idempotency_key=idempotency_key).count()
        print(f"[VERIFY] Transaction rows with this idempotency_key: {count} (expected 1)")
        print(f"[VERIFY] Row id={row.id}, matches winning response id={winning_transaction_id}: "
              f"{str(row.id) == winning_transaction_id}")

        if count == 1 and str(row.id) == winning_transaction_id:
            print("\n[RESULT] PASS — the Redis race is fixed: exactly one request "
                  "was accepted, exactly one Transaction row exists, and the "
                  "winning client's transaction_id matches it exactly.")
        else:
            print("\n[RESULT] FAIL — see mismatches above.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
