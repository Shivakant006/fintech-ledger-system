"""
Manual verification script for the reconciliation check: crud.reconcile_accounts()
must flag any account whose stored balance disagrees with
opening_balance + SUM(ledger_lines.amount), and must stay quiet otherwise.

Run with:
    docker compose exec api python -m app.test_reconciliation

A check that returns "all clear" is only trustworthy if we've seen it return
"NOT clear" when we deliberately break something. So this script corrupts a
balance on purpose (bypassing the ledger, the way a manual UPDATE or a future
bug would) and confirms the check catches it.

SCENARIO 1: a normal payment leaves both accounts reconciled.
SCENARIO 2: corrupting a balance is detected, with the right difference, and
            the flag clears once the corruption is undone.
SCENARIO 3: an account with NO ledger lines is still checked. This is what the
            outer join is for: an inner join would silently skip such accounts.

Only the accounts created by this script are inspected, so leftover data from
other test scripts can't affect the result.
"""
import sys
import uuid

from . import models, schemas, crud
from .database import SessionLocal, engine, Base


def _make_accounts(user_opening, merchant_opening=0):
    """Create a user + merchant whose balance equals their opening_balance."""
    db = SessionLocal()
    try:
        run_id = uuid.uuid4().hex[:8]
        user = models.Account(name=f"Recon User {run_id}",
                              type=models.AccountType.USER,
                              balance=user_opening, opening_balance=user_opening)
        merchant = models.Account(name=f"Recon Merchant {run_id}",
                                  type=models.AccountType.MERCHANT,
                                  balance=merchant_opening, opening_balance=merchant_opening)
        db.add_all([user, merchant])
        db.commit()
        db.refresh(user)
        db.refresh(merchant)
        return user.id, merchant.id
    finally:
        db.close()


def _flagged(account_ids):
    """Run the real check, keep only the accounts this script created."""
    db = SessionLocal()
    try:
        return {row["account_id"]: row
                for row in crud.reconcile_accounts(db)
                if row["account_id"] in account_ids}
    finally:
        db.close()


def _shift_balance(account_id, delta):
    """Change a stored balance directly, bypassing the ledger (the 'corruption')."""
    db = SessionLocal()
    try:
        db.query(models.Account).filter(models.Account.id == account_id).update(
            {"balance": models.Account.balance + delta})
        db.commit()
    finally:
        db.close()


def run_scenario_1():
    print("\n=== SCENARIO 1: normal payment stays reconciled ===")
    user_id, merchant_id = _make_accounts(user_opening=10_000)
    payment = schemas.PaymentTask(
        transaction_id=uuid.uuid4(),
        idempotency_key=f"test-recon-{uuid.uuid4().hex[:8]}",
        user_account_id=user_id,
        merchant_account_id=merchant_id,
        amount_cents=3_000,
        description="Reconciliation test payment",
    )
    db = SessionLocal()
    try:
        crud.execute_payment(db=db, payment_data=payment)
    finally:
        db.close()
    print("[SETUP] Settled a 3000 payment through the real execute_payment().")

    flagged = _flagged({user_id, merchant_id})
    if flagged:
        print(f"[VERIFY] !! FAIL: clean accounts were flagged: {list(flagged.values())}")
        return False
    print("[VERIFY] Neither account flagged.")
    print("[RESULT] PASS")
    return True


def run_scenario_2():
    print("\n=== SCENARIO 2: corrupted balance is detected ===")
    user_id, merchant_id = _make_accounts(user_opening=10_000)
    payment = schemas.PaymentTask(
        transaction_id=uuid.uuid4(),
        idempotency_key=f"test-recon-{uuid.uuid4().hex[:8]}",
        user_account_id=user_id,
        merchant_account_id=merchant_id,
        amount_cents=3_000,
        description="Reconciliation test payment",
    )
    db = SessionLocal()
    try:
        crud.execute_payment(db=db, payment_data=payment)
    finally:
        db.close()

    corruption = 777
    _shift_balance(user_id, corruption)
    print(f"[SETUP] Added {corruption} to the user's balance, bypassing the ledger.")
    try:
        flagged = _flagged({user_id, merchant_id})

        if user_id not in flagged:
            print("[VERIFY] !! FAIL: the corrupted account was NOT flagged.")
            return False
        row = flagged[user_id]
        if row["difference"] != corruption:
            print(f"[VERIFY] !! FAIL: flagged, but difference={row['difference']}, "
                  f"expected {corruption}.")
            return False
        print(f"[VERIFY] Corrupted account flagged: stored={row['stored_balance']}, "
              f"expected={row['expected_balance']}, difference={row['difference']}")

        if merchant_id in flagged:
            print("[VERIFY] !! FAIL: the untouched merchant was flagged too.")
            return False
        print("[VERIFY] Untouched merchant not flagged.")
    finally:
        _shift_balance(user_id, -corruption)  # leave the database clean

    if _flagged({user_id, merchant_id}):
        print("[VERIFY] !! FAIL: still flagged after undoing the corruption.")
        return False
    print("[VERIFY] Flag cleared after the corruption was undone.")
    print("[RESULT] PASS")
    return True


def run_scenario_3():
    print("\n=== SCENARIO 3: account with NO ledger lines is still checked ===")
    user_id, merchant_id = _make_accounts(user_opening=500)
    print("[SETUP] Accounts created with no payments, so no ledger lines exist.")

    if _flagged({user_id, merchant_id}):
        print("[VERIFY] !! FAIL: fresh accounts flagged before any corruption.")
        return False

    corruption = 100
    _shift_balance(user_id, corruption)
    print(f"[SETUP] Added {corruption} to the user's balance.")
    try:
        flagged = _flagged({user_id, merchant_id})
        if user_id not in flagged:
            print("[VERIFY] !! FAIL: account with no ledger lines was NOT flagged. "
                  "(An inner join would cause exactly this.)")
            return False
        row = flagged[user_id]
        if row["expected_balance"] != 500 or row["difference"] != corruption:
            print(f"[VERIFY] !! FAIL: wrong numbers: {row}")
            return False
        print(f"[VERIFY] Flagged correctly: stored={row['stored_balance']}, "
              f"expected={row['expected_balance']}, difference={row['difference']}")
    finally:
        _shift_balance(user_id, -corruption)

    print("[RESULT] PASS")
    return True


def main():
    Base.metadata.create_all(bind=engine)
    results = [
        ("Scenario 1 (normal payment stays clean)        ", run_scenario_1()),
        ("Scenario 2 (corruption detected, then cleared) ", run_scenario_2()),
        ("Scenario 3 (account with no ledger lines)      ", run_scenario_3()),
    ]
    print("\n=== SUMMARY ===")
    for label, ok in results:
        print(f"{label}: {'PASS' if ok else 'FAIL'}")
    if not all(ok for _, ok in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
    