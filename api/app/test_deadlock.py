"""
Manual verification script for the deadlock-prevention fix (ORDER BY on the
FOR UPDATE query in crud.execute_payment).

This is NOT pytest — run it once, inside the running `api` container:
    docker compose exec api python -m app.test_deadlock

IMPORTANT: this script does NOT call crud.execute_payment(). It works one
layer below, with raw SQL locks, on purpose. Here's why:

  execute_payment() locks both accounts in a SINGLE query:
      .filter(id.in_([a, b])).order_by(id).with_for_update()
  Postgres almost always picks the same scan plan for the same query, so
  even the BROKEN version (no ORDER BY) would often lock rows in the same
  order by coincidence, most of the time. That's exactly what makes the bug
  dangerous — it's not "broken every run," it's "correct by accident until
  the day it isn't" (different data volume, a changed query plan, etc.).

  So instead of hoping to get unlucky enough to catch a real deadlock, this
  script FORCES the two failure/success scenarios directly and
  deterministically, using two raw locks per thread with an explicit
  synchronization barrier:

  SCENARIO 1 — "broken": two threads lock the SAME two rows in OPPOSITE
  order (thread A: row1 then row2; thread B: row2 then row1), with a
  barrier guaranteeing both threads are holding their first lock before
  either reaches for its second. This is a textbook circular wait — it
  WILL deadlock, every time. This proves the failure mode is real.

  SCENARIO 2 — "fixed": both threads lock the SAME two rows in the SAME
  order (both: lower id first, then higher id) — mirroring what
  ORDER BY(id) guarantees in the real query. One thread simply waits its
  turn for the first row; there is no circular wait, so no deadlock is
  even possible. This proves the fix's actual mechanism, not just that we
  wrote the word "order_by" somewhere.
"""
import threading
import time
import uuid

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from . import models
from .database import SessionLocal, engine, Base


def _setup_accounts():
    """Create two fresh accounts so this script can be re-run safely."""
    db = SessionLocal()
    try:
        run_id = uuid.uuid4().hex[:8]
        acc1 = models.Account(name=f"Deadlock Test A {run_id}",
                               type=models.AccountType.USER, balance=1000)
        acc2 = models.Account(name=f"Deadlock Test B {run_id}",
                               type=models.AccountType.USER, balance=1000)
        db.add_all([acc1, acc2])
        db.commit()
        db.refresh(acc1)
        db.refresh(acc2)
        return acc1.id, acc2.id
    finally:
        db.close()


def _is_deadlock_error(e: OperationalError) -> bool:
    # Postgres SQLSTATE 40P01 = deadlock_detected. Checking the code is more
    # reliable than string-matching the message.
    return getattr(e.orig, "pgcode", None) == "40P01" or "deadlock detected" in str(e).lower()


def _lock_row(conn, account_id):
    conn.execute(
        text("SELECT * FROM accounts WHERE id = :id FOR UPDATE"),
        {"id": str(account_id)},
    )


def _worker(first_id, second_id, barrier, results, idx, hold_seconds):
    """Locks `first_id`, optionally waits at a barrier, then locks
    `second_id`, then commits. Records the outcome in results[idx]."""
    conn = engine.connect()
    trans = conn.begin()
    try:
        _lock_row(conn, first_id)

        if barrier is not None:
            # Don't proceed to the second lock until BOTH threads are
            # confirmed to be holding their first lock. This is what makes
            # the "broken" scenario a guaranteed, deterministic deadlock
            # instead of a rare race we'd have to get lucky to observe.
            barrier.wait()
        else:
            # "fixed" scenario: no artificial synchronization needed — a
            # brief hold just makes the serialization (thread 2 waiting on
            # thread 1) visible rather than instantaneous.
            time.sleep(hold_seconds)

        _lock_row(conn, second_id)
        trans.commit()
        results[idx] = "success"
    except OperationalError as e:
        trans.rollback()
        results[idx] = "deadlock" if _is_deadlock_error(e) else f"error: {e}"
    finally:
        conn.close()


def run_broken_scenario(acc1, acc2):
    print("\n=== SCENARIO 1: locking in OPPOSITE order (the old bug) ===")
    barrier = threading.Barrier(2)
    results = [None, None]
    t1 = threading.Thread(target=_worker, args=(acc1, acc2, barrier, results, 0, 0))
    t2 = threading.Thread(target=_worker, args=(acc2, acc1, barrier, results, 1, 0))
    t1.start(); t2.start()
    t1.join(timeout=10); t2.join(timeout=10)

    print(f"Thread 1 (locked acc1 -> acc2): {results[0]}")
    print(f"Thread 2 (locked acc2 -> acc1): {results[1]}")

    if "deadlock" in results:
        print("[RESULT] PASS — opposite lock order DID deadlock, as expected. "
              "This confirms the mechanism the bug relied on is real.")
        return True
    else:
        print("[RESULT] UNEXPECTED — no deadlock occurred. (Rare, but not "
              "impossible depending on timing/Postgres version — the barrier "
              "should make this reliable.)")
        return False


def run_fixed_scenario(acc1, acc2):
    print("\n=== SCENARIO 2: locking in the SAME order (the fix) ===")
    low_id, high_id = sorted([acc1, acc2])  # mirrors ORDER BY(id) ascending
    results = [None, None]
    # No barrier: both threads attempt low_id first. Whichever loses the
    # race simply blocks on that first SELECT until the winner commits —
    # there is no scenario where each thread ends up holding one lock and
    # waiting on the other, so no deadlock is structurally possible here.
    t1 = threading.Thread(target=_worker, args=(low_id, high_id, None, results, 0, 0.5))
    t2 = threading.Thread(target=_worker, args=(low_id, high_id, None, results, 1, 0.5))
    t1.start(); t2.start()
    t1.join(timeout=10); t2.join(timeout=10)

    print(f"Thread 1 (locked low -> high): {results[0]}")
    print(f"Thread 2 (locked low -> high): {results[1]}")

    if results[0] == "success" and results[1] == "success":
        print("[RESULT] PASS — same lock order, no deadlock. Both transactions "
              "completed; one simply waited its turn.")
        return True
    else:
        print("[RESULT] FAIL — expected both threads to succeed.")
        return False


def main():
    Base.metadata.create_all(bind=engine)
    acc1, acc2 = _setup_accounts()
    print(f"[SETUP] account 1 = {acc1}")
    print(f"[SETUP] account 2 = {acc2}")

    broken_ok = run_broken_scenario(acc1, acc2)
    fixed_ok = run_fixed_scenario(acc1, acc2)

    print("\n=== SUMMARY ===")
    print(f"Opposite order really does deadlock : {'YES' if broken_ok else 'NOT CONFIRMED'}")
    print(f"Same order (the fix) avoids it       : {'YES' if fixed_ok else 'NO'}")


if __name__ == "__main__":
    main()
