"""
Manual verification script for single-currency (INR-only) enforcement.

The ledger is INR-only, enforced by the database itself: accounts.currency is
NOT NULL with a server default of 'INR', and a CHECK constraint
(check_currency_inr) rejects any other value. Because it lives in Postgres,
no code path, raw SQL insert, or manual UPDATE can create a non-INR account.

Run with:
    docker compose exec api python -m app.test_currency_validation

SCENARIO 1: inserting a USD account is rejected by check_currency_inr.
SCENARIO 2: inserting an account with a NULL currency is rejected (NOT NULL).
SCENARIO 3: a raw SQL insert that omits currency gets 'INR' from the server
            default (this is how the README seed SQL inserts accounts).
SCENARIO 4: changing an existing account's currency to USD with a raw UPDATE
            is rejected too, so the rule can't be bypassed after creation.
SCENARIO 5: a normal INR payment still succeeds and the books stay reconciled.

Raw SQL is used on purpose in scenarios 1-4: it bypasses the Python model, so
a pass proves the DATABASE is enforcing the rule, not just our application code.
"""
import sys
import uuid

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from . import models, schemas, crud
from .database import SessionLocal, engine, Base


def _try_sql(sql, params):
    """Run one statement. Returns (accepted, first_line_of_error_or_empty)."""
    db = SessionLocal()
    try:
        db.execute(text(sql), params)
        db.commit()
        return True, ""
    except IntegrityError as e:
        db.rollback()
        return False, str(e.orig).strip().splitlines()[0]
    finally:
        db.close()


def _insert_params(**extra):
    return {"id": str(uuid.uuid4()), "name": f"Currency Test {uuid.uuid4().hex[:8]}", **extra}


def run_scenario_1():
    print("\n=== SCENARIO 1: a USD account is rejected ===")
    accepted, err = _try_sql(
        "INSERT INTO accounts (id, name, type, balance, opening_balance, currency) "
        "VALUES (:id, :name, 'USER', 0, 0, 'USD')",
        _insert_params(),
    )
    if accepted:
        print("[VERIFY] !! FAIL: a USD account was ACCEPTED.")
        return False
    if "check_currency_inr" not in err:
        print(f"[VERIFY] !! FAIL: rejected, but not by the currency constraint: {err}")
        return False
    print(f"[VERIFY] Rejected by the database: {err}")
    print("[RESULT] PASS")
    return True


def run_scenario_2():
    print("\n=== SCENARIO 2: a NULL currency is rejected ===")
    accepted, err = _try_sql(
        "INSERT INTO accounts (id, name, type, balance, opening_balance, currency) "
        "VALUES (:id, :name, 'USER', 0, 0, NULL)",
        _insert_params(),
    )
    if accepted:
        print("[VERIFY] !! FAIL: an account with NULL currency was ACCEPTED.")
        return False
    if "currency" not in err:
        print(f"[VERIFY] !! FAIL: rejected, but not because of currency: {err}")
        return False
    print(f"[VERIFY] Rejected by the database: {err}")
    print("[RESULT] PASS")
    return True


def run_scenarios_3_and_4():
    print("\n=== SCENARIO 3: omitting currency in raw SQL defaults to INR ===")
    params = _insert_params()
    accepted, err = _try_sql(
        "INSERT INTO accounts (id, name, type, balance, opening_balance) "
        "VALUES (:id, :name, 'USER', 0, 0)",
        params,
    )
    if not accepted:
        print(f"[VERIFY] !! FAIL: a normal insert without currency was rejected: {err}")
        return False, False

    ok3 = ok4 = False
    db = SessionLocal()
    try:
        currency = db.execute(
            text("SELECT currency FROM accounts WHERE id = :id"), {"id": params["id"]}
        ).scalar()
        if currency == "INR":
            print(f"[VERIFY] Row stored with currency={currency!r} from the server default.")
            print("[RESULT] PASS")
            ok3 = True
        else:
            print(f"[VERIFY] !! FAIL: expected 'INR', got {currency!r}.")

        print("\n=== SCENARIO 4: raw UPDATE to USD is rejected ===")
        accepted, err = _try_sql(
            "UPDATE accounts SET currency = 'USD' WHERE id = :id", {"id": params["id"]}
        )
        if accepted:
            print("[VERIFY] !! FAIL: UPDATE to USD was ACCEPTED.")
        elif "check_currency_inr" not in err:
            print(f"[VERIFY] !! FAIL: rejected, but not by the currency constraint: {err}")
        else:
            print(f"[VERIFY] Rejected by the database: {err}")
            print("[RESULT] PASS")
            ok4 = True
    finally:
        db.execute(text("DELETE FROM accounts WHERE id = :id"), {"id": params["id"]})
        db.commit()
        db.close()
    return ok3, ok4


def run_scenario_5():
    print("\n=== SCENARIO 5: a normal INR payment still works ===")
    db = SessionLocal()
    try:
        run_id = uuid.uuid4().hex[:8]
        user = models.Account(name=f"Currency User {run_id}", type=models.AccountType.USER,
                              balance=10_000, opening_balance=10_000)
        merchant = models.Account(name=f"Currency Merchant {run_id}", type=models.AccountType.MERCHANT,
                                  balance=0, opening_balance=0)
        db.add_all([user, merchant])
        db.commit()
        db.refresh(user)
        db.refresh(merchant)
        user_id, merchant_id = user.id, merchant.id

        payment = schemas.PaymentTask(
            transaction_id=uuid.uuid4(),
            idempotency_key=f"test-currency-{uuid.uuid4().hex[:8]}",
            user_account_id=user_id,
            merchant_account_id=merchant_id,
            amount_cents=2_500,
            description="Currency test payment",
        )
        crud.execute_payment(db=db, payment_data=payment)
        print("[SETUP] Settled a 2500 payment through the real execute_payment().")

        flagged = [r for r in crud.reconcile_accounts(db)
                   if r["account_id"] in (user_id, merchant_id)]
        if flagged:
            print(f"[VERIFY] !! FAIL: accounts flagged by reconciliation: {flagged}")
            return False
        print("[VERIFY] Payment settled and reconciliation is clean.")
        print("[RESULT] PASS")
        return True
    finally:
        db.close()


def main():
    Base.metadata.create_all(bind=engine)
    r1 = run_scenario_1()
    r2 = run_scenario_2()
    r3, r4 = run_scenarios_3_and_4()
    r5 = run_scenario_5()
    results = [
        ("Scenario 1 (USD account rejected)          ", r1),
        ("Scenario 2 (NULL currency rejected)        ", r2),
        ("Scenario 3 (server default gives INR)      ", r3),
        ("Scenario 4 (UPDATE to USD rejected)        ", r4),
        ("Scenario 5 (normal INR payment still works)", r5),
    ]
    print("\n=== SUMMARY ===")
    for label, ok in results:
        print(f"{label}: {'PASS' if ok else 'FAIL'}")
    if not all(ok for _, ok in results):
        sys.exit(1)


if __name__ == "__main__":
    main()