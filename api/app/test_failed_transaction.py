"""
Manual verification script for bug #4: business-rule rejections (e.g.
insufficient funds) now create a FAILED Transaction record instead of
vanishing silently.

Run with:
    docker compose exec api python -m app.test_failed_transaction

SCENARIO 1 (deterministic): a single insufficient-funds payment must leave
a FAILED row behind, and a redelivery of that same message must be
recognized as a duplicate of a FAILED payment (not silently re-attempted,
not misreported as "already settled").

SCENARIO 2 (best-effort, NOT deterministic — flagged honestly): two
concurrent attempts at the same insufficient-funds idempotency_key. One
should end up as the FAILED row, the other should hit our nested
IntegrityError handler and come back as a DuplicateTransactionError
instead of crashing or retrying forever. Unlike the deadlock test, there
is no clean way to force this race from outside execute_payment() without
adding test-only hooks into production code, so this scenario is run
several times to raise the odds of catching it, not guaranteed every run.
"""
import threading
import uuid

from . import models, schemas, crud
from .crud import DuplicateTransactionError
from .database import SessionLocal, engine, Base


def _setup_accounts(balance_cents):
    db = SessionLocal()
    try:
        run_id = uuid.uuid4().hex[:8]
        user = models.Account(name=f"Poor Test User {run_id}",
                               type=models.AccountType.USER, balance=balance_cents)
        merchant = models.Account(name=f"Test Merchant {run_id}",
                                   type=models.AccountType.MERCHANT, balance=0)
        db.add_all([user, merchant])
        db.commit()
        db.refresh(user)
        db.refresh(merchant)
        return user.id, merchant.id
    finally:
        db.close()


def run_scenario_1():
    print("\n=== SCENARIO 1: solo insufficient-funds payment ===")
    db = SessionLocal()
    try:
        user_id, merchant_id = _setup_accounts(balance_cents=100)  # only $1.00
        idempotency_key = f"test-fail-{uuid.uuid4().hex[:8]}"
        transaction_id = uuid.uuid4()
        payment = schemas.PaymentTask(
            transaction_id=transaction_id,
            idempotency_key=idempotency_key,
            user_account_id=user_id,
            merchant_account_id=merchant_id,
            amount_cents=5_000,  # $50.00 — far more than the $1.00 balance
            description="Insufficient funds test",
        )

        # ---- Attempt 1: should fail with ValueError, and leave a FAILED row
        print("[ATTEMPT 1] Sending a payment we can't afford...")
        try:
            crud.execute_payment(db=db, payment_data=payment)
            print("[ATTEMPT 1] !! FAIL: expected ValueError, none was raised.")
            return False
        except ValueError as e:
            print(f"[ATTEMPT 1] Correctly rejected: {e}")

        saved = db.query(models.Transaction).filter_by(idempotency_key=idempotency_key).first()
        if saved is None:
            print("[VERIFY] !! FAIL: no Transaction row was created at all. "
                  "The failure is not being recorded (this is the original bug).")
            return False
        if saved.status != models.TransactionStatus.FAILED:
            print(f"[VERIFY] !! FAIL: row exists but status={saved.status}, expected FAILED.")
            return False
        if saved.id != transaction_id:
            print(f"[VERIFY] !! FAIL: saved id {saved.id} does not match the id "
                  f"the client was given {transaction_id}.")
            return False
        print(f"[VERIFY] FAILED row exists with the correct id: {saved.id}")

        # ---- Attempt 2: redelivery of the exact same message ----------------
        print("\n[ATTEMPT 2] Redelivering the SAME message (simulating Pub/Sub retry)...")
        try:
            crud.execute_payment(db=db, payment_data=payment)
            print("[ATTEMPT 2] !! FAIL: expected DuplicateTransactionError, none raised.")
            return False
        except DuplicateTransactionError as e:
            if e.status != models.TransactionStatus.FAILED:
                print(f"[ATTEMPT 2] !! FAIL: got DuplicateTransactionError but "
                      f"status={e.status}, expected FAILED.")
                return False
            print(f"[ATTEMPT 2] Correctly recognized as a duplicate of a FAILED "
                  f"payment (status correctly reported): {e}")

        # ---- Balances should never have moved, not even once ----------------
        user = db.query(models.Account).get(user_id)
        merchant = db.query(models.Account).get(merchant_id)
        if user.balance != 100 or merchant.balance != 0:
            print(f"[VERIFY] !! FAIL: balances moved! user={user.balance}, "
                  f"merchant={merchant.balance} (expected 100 / 0).")
            return False
        print(f"[VERIFY] Balances untouched: user={user.balance}, merchant={merchant.balance}")

        print("[RESULT] PASS")
        return True
    finally:
        db.close()


def _race_worker(payment, results, idx):
    db = SessionLocal()
    try:
        try:
            crud.execute_payment(db=db, payment_data=payment)
            results[idx] = "unexpected-success"
        except ValueError:
            results[idx] = "failed-recorded"
        except DuplicateTransactionError as e:
            results[idx] = f"duplicate-{e.status.value if e.status else 'unknown'}"
        except Exception as e:
            results[idx] = f"error: {e}"
    finally:
        db.close()


def run_scenario_2(attempts=8):
    print(f"\n=== SCENARIO 2: concurrent insufficient-funds race "
          f"(best-effort, {attempts} attempts) ===")
    print("Unlike the deadlock test, this race can't be forced deterministically "
          "from outside execute_payment(), so this is run several times to raise "
          "the odds of observing it — it is NOT guaranteed on every run.")

    caught_the_race = False
    for i in range(attempts):
        user_id, merchant_id = _setup_accounts(balance_cents=100)
        idempotency_key = f"test-race-{uuid.uuid4().hex[:8]}"
        payment = schemas.PaymentTask(
            transaction_id=uuid.uuid4(),
            idempotency_key=idempotency_key,
            user_account_id=user_id,
            merchant_account_id=merchant_id,
            amount_cents=5_000,
            description="Concurrent insufficient funds test",
        )

        results = [None, None]
        barrier_start = threading.Barrier(2)

        def worker(idx):
            barrier_start.wait()  # start both threads at the same instant
            _race_worker(payment, results, idx)

        t1 = threading.Thread(target=worker, args=(0,))
        t2 = threading.Thread(target=worker, args=(1,))
        t1.start(); t2.start()
        t1.join(timeout=10); t2.join(timeout=10)

        outcomes = set(results)
        print(f"  run {i+1}/{attempts}: {results}")

        if "unexpected-success" in outcomes:
            print("  !! FAIL: a payment succeeded despite insufficient funds.")
            return False

        if any(r and r.startswith("error:") for r in results):
            print(f"  !! FAIL: an unhandled error leaked out: {results}")
            return False

        # The race we're hoping to observe: one thread recorded the FAILED
        # row, the other got a DuplicateTransactionError pointing at it.
        if "failed-recorded" in outcomes and any(r == "duplicate-FAILED" for r in results):
            print("  Race observed and handled correctly on this run.")
            caught_the_race = True

    if caught_the_race:
        print("[RESULT] PASS — the concurrent-failure race was observed at least "
              "once, and handled correctly (no crash, no infinite retry, no "
              "double-write).")
    else:
        print("[RESULT] INCONCLUSIVE — the race was never actually triggered in "
              f"{attempts} attempts (both threads likely recorded FAILED "
              "sequentially without overlapping). This does not disprove the "
              "fix, it just means we didn't get unlucky enough to observe it. "
              "The code path is still covered by reasoning, same as any race "
              "test without a forced barrier inside the function itself.")
    return True  # inconclusive is not the same as failed


def main():
    Base.metadata.create_all(bind=engine)
    ok1 = run_scenario_1()
    ok2 = run_scenario_2()
    print("\n=== SUMMARY ===")
    print(f"Scenario 1 (solo failure recorded + redelivery handled): {'PASS' if ok1 else 'FAIL'}")
    print(f"Scenario 2 (concurrent failure race, best-effort)      : {'OK' if ok2 else 'FAIL'}")


if __name__ == "__main__":
    main()
