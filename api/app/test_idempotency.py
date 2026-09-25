"""
Manual verification script for the idempotency fix in crud.execute_payment.

This is NOT pytest — it's a plain script you run once, inside the running
`api` container, against the real Postgres from docker-compose. It proves,
step by step, that sending the same payment twice (same idempotency_key)
settles it exactly once.

Run it with:
    docker compose exec api python -m app.test_idempotency

What it does:
  1. Creates two fresh accounts (a USER and a MERCHANT) with a starting
     balance, so this script can be re-run without colliding with old data.
  2. Calls execute_payment() ONCE — this should succeed normally.
  3. Calls execute_payment() AGAIN with the exact same idempotency_key —
     this simulates a Pub/Sub redelivery. This should raise
     DuplicateTransactionError, NOT move money a second time.
  4. Re-reads the accounts from the database and asserts the balances only
     moved once, and that only ONE Transaction row exists for that key.
"""
import uuid
from . import models, schemas, crud
from .crud import DuplicateTransactionError
from .database import SessionLocal, engine, Base


def main():
    # Make sure tables exist (no-op if alembic already created them).
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        # ---- 1. Set up two fresh accounts ----------------------------------
        run_id = uuid.uuid4().hex[:8]
        user = models.Account(
            name=f"Test User {run_id}",
            type=models.AccountType.USER,
            balance=10_000,  # $100.00, in cents
        )
        merchant = models.Account(
            name=f"Test Merchant {run_id}",
            type=models.AccountType.MERCHANT,
            balance=0,
        )
        db.add_all([user, merchant])
        db.commit()
        db.refresh(user)
        db.refresh(merchant)

        print(f"[SETUP] user={user.id} balance={user.balance}")
        print(f"[SETUP] merchant={merchant.id} balance={merchant.balance}")

        idempotency_key = f"test-key-{run_id}"
        expected_transaction_id = uuid.uuid4()
        payment = schemas.PaymentTask(
            transaction_id=expected_transaction_id,
            idempotency_key=idempotency_key,
            user_account_id=user.id,
            merchant_account_id=merchant.id,
            amount_cents=2_500,  # $25.00
            description="Idempotency test payment",
        )

        # ---- 2. First attempt — should succeed ------------------------------
        print("\n[ATTEMPT 1] Sending payment for the first time...")
        txn1 = crud.execute_payment(db=db, payment_data=payment)
        assert txn1.id == expected_transaction_id, (
            "BUG: the transaction was saved with a DIFFERENT id than the one "
            "generated and handed to the client — this is the phantom "
            "transaction_id bug."
        )
        print(f"[ATTEMPT 1] id matches what the client was given: {txn1.id}")
        print(f"[ATTEMPT 1] SUCCESS. Transaction id={txn1.id}")

        # ---- 3. Second attempt, SAME idempotency_key — simulates redelivery -
        print("\n[ATTEMPT 2] Sending the SAME payment again (simulating a "
              "Pub/Sub redelivery)...")
        try:
            crud.execute_payment(db=db, payment_data=payment)
            print("[ATTEMPT 2] !! FAIL: expected DuplicateTransactionError, "
                  "but no exception was raised. The bug is NOT fixed.")
            raise SystemExit(1)
        except DuplicateTransactionError as e:
            print(f"[ATTEMPT 2] Correctly rejected as duplicate: {e}")

        # ---- 4. Verify the actual state in the database ---------------------
        db.refresh(user)
        db.refresh(merchant)
        print(f"\n[VERIFY] user balance   = {user.balance} (expected 7500)")
        print(f"[VERIFY] merchant balance = {merchant.balance} (expected 2500)")

        txn_count = (
            db.query(models.Transaction)
            .filter_by(idempotency_key=idempotency_key)
            .count()
        )
        print(f"[VERIFY] transactions with this idempotency_key = {txn_count} (expected 1)")

        assert user.balance == 10_000 - 2_500, "User balance moved more than once!"
        assert merchant.balance == 2_500, "Merchant balance moved more than once!"
        assert txn_count == 1, "More than one Transaction row was created for the same key!"

        print("\n[RESULT] PASS — duplicate delivery did not double-settle the payment.")

    finally:
        db.close()


if __name__ == "__main__":
    main()
