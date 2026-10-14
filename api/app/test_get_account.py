"""
Manual verification script for GET /accounts/{account_id}.

Calls the LIVE API (like test_race_condition.py), so run it with:
    docker compose exec api python -m app.test_get_account

SCENARIO 1: an existing account returns 200 with exactly the five documented
            fields, and does NOT leak opening_balance.
SCENARIO 2: after a payment settles, the endpoint shows the updated balances.
SCENARIO 3: an unknown account id returns 404.
SCENARIO 4: a malformed id returns 422 (FastAPI validation), not a 500.
"""
import json
import sys
import urllib.error
import urllib.request
import uuid

from . import models, schemas, crud
from .database import SessionLocal, engine, Base

BASE_URL = "http://localhost:8000/accounts"
EXPECTED_FIELDS = {"account_id", "name", "type", "balance_cents", "currency"}


def _get(account_id):
    """GET /accounts/{id}. Returns (status_code, parsed_json_or_text)."""
    try:
        with urllib.request.urlopen(f"{BASE_URL}/{account_id}") as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        try:
            body = json.loads(body)
        except ValueError:
            pass
        return e.code, body


def _make_accounts(user_opening, merchant_opening=0):
    db = SessionLocal()
    try:
        run_id = uuid.uuid4().hex[:8]
        user = models.Account(name=f"GetAcct User {run_id}", type=models.AccountType.USER,
                              balance=user_opening, opening_balance=user_opening)
        merchant = models.Account(name=f"GetAcct Merchant {run_id}", type=models.AccountType.MERCHANT,
                                  balance=merchant_opening, opening_balance=merchant_opening)
        db.add_all([user, merchant])
        db.commit()
        db.refresh(user)
        db.refresh(merchant)
        return user.id, user.name, merchant.id
    finally:
        db.close()


def run_scenario_1():
    print("\n=== SCENARIO 1: existing account returns the documented fields ===")
    user_id, user_name, _ = _make_accounts(user_opening=10_000)
    code, body = _get(user_id)
    if code != 200:
        print(f"[VERIFY] !! FAIL: expected 200, got {code}: {body}")
        return False
    if set(body) != EXPECTED_FIELDS:
        print(f"[VERIFY] !! FAIL: fields {sorted(body)} != expected {sorted(EXPECTED_FIELDS)}")
        return False
    expected = {"account_id": str(user_id), "name": user_name, "type": "USER",
                "balance_cents": 10_000, "currency": "INR"}
    if body != expected:
        print(f"[VERIFY] !! FAIL: got {body}, expected {expected}")
        return False
    print(f"[VERIFY] 200 with exactly the five fields: {body}")
    print("[VERIFY] opening_balance is not exposed.")
    print("[RESULT] PASS")
    return True


def run_scenario_2():
    print("\n=== SCENARIO 2: balances update after a payment settles ===")
    user_id, _, merchant_id = _make_accounts(user_opening=10_000)
    payment = schemas.PaymentTask(
        transaction_id=uuid.uuid4(),
        idempotency_key=f"test-getacct-{uuid.uuid4().hex[:8]}",
        user_account_id=user_id,
        merchant_account_id=merchant_id,
        amount_cents=2_500,
        description="GET account test payment",
    )
    db = SessionLocal()
    try:
        crud.execute_payment(db=db, payment_data=payment)
    finally:
        db.close()
    print("[SETUP] Settled a 2500 payment through the real execute_payment().")

    ok = True
    for acct_id, label, want in ((user_id, "user", 7_500), (merchant_id, "merchant", 2_500)):
        code, body = _get(acct_id)
        got = body.get("balance_cents") if isinstance(body, dict) else None
        if code != 200 or got != want:
            print(f"[VERIFY] !! FAIL: {label} status={code} balance_cents={got}, expected {want}")
            ok = False
        else:
            print(f"[VERIFY] {label} balance_cents={got} (expected {want})")
    print(f"[RESULT] {'PASS' if ok else 'FAIL'}")
    return ok


def run_scenario_3():
    print("\n=== SCENARIO 3: unknown account id returns 404 ===")
    code, body = _get(uuid.uuid4())
    if code != 404:
        print(f"[VERIFY] !! FAIL: expected 404, got {code}: {body}")
        return False
    print(f"[VERIFY] 404 as expected: {body}")
    print("[RESULT] PASS")
    return True


def run_scenario_4():
    print("\n=== SCENARIO 4: malformed id returns 422, not a 500 ===")
    code, body = _get("not-a-uuid")
    if code != 422:
        print(f"[VERIFY] !! FAIL: expected 422, got {code}: {body}")
        return False
    print("[VERIFY] 422 as expected (rejected by request validation).")
    print("[RESULT] PASS")
    return True


def main():
    Base.metadata.create_all(bind=engine)
    results = [
        ("Scenario 1 (documented fields, no leak)  ", run_scenario_1()),
        ("Scenario 2 (balances after settlement)   ", run_scenario_2()),
        ("Scenario 3 (unknown id -> 404)           ", run_scenario_3()),
        ("Scenario 4 (malformed id -> 422)         ", run_scenario_4()),
    ]
    print("\n=== SUMMARY ===")
    for label, ok in results:
        print(f"{label}: {'PASS' if ok else 'FAIL'}")
    if not all(ok for _, ok in results):
        sys.exit(1)


if __name__ == "__main__":
    main()