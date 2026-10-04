from uuid import UUID
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from . import models, schemas


def get_transaction(db: Session, transaction_id: UUID) -> models.Transaction | None:
    """
    Simple read: look up a single Transaction by id. No locking, no
    business logic — a status check is not a money-moving operation, so
    none of the concurrency protections execute_payment() needs apply
    here. Returns None if no matching row exists; the caller (the API
    endpoint) decides what that means (a 404).
    """
    return db.query(models.Transaction).filter(models.Transaction.id == transaction_id).first()


class DuplicateTransactionError(Exception):
    """Raised when a transaction with this idempotency_key has already been
    resolved — either COMPLETED or FAILED. Either way, this exact request
    was already handled once; the caller should treat this as a no-op, not
    retry the business logic. Carries the existing record's status so
    callers (e.g. the worker's logging) can tell the two cases apart
    instead of assuming "already settled" for every duplicate."""
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


def execute_payment(db: Session, payment_data: schemas.PaymentTask) -> models.Transaction:
    # A try/except block is mandatory. If any step fails, we MUST rollback.
    try:
        # -------------------------------------------------------------------
        # 0. IDEMPOTENCY CHECK (proactive path)
        # Pub/Sub is at-least-once delivery: the same message can reach us
        # more than once. If we've already resolved this idempotency_key —
        # whether it succeeded OR failed a business rule — this is a
        # redelivery, not a new attempt. Handle it here, before we touch
        # any account rows. (Replaying an idempotency key never re-runs
        # the business logic, the same way Stripe's idempotency keys work:
        # a genuinely new attempt should use a NEW key.)
        # -------------------------------------------------------------------
        existing = db.query(models.Transaction).filter_by(
            idempotency_key=payment_data.idempotency_key
        ).first()
        if existing:
            raise DuplicateTransactionError(
                f"Transaction with idempotency_key={payment_data.idempotency_key} "
                f"already exists (id={existing.id}, status={existing.status.value}).",
                status=existing.status,
            )

        # -------------------------------------------------------------------
        # 1 & 2. THE ROW LOCK (SELECT FOR UPDATE) — PREVENT DEADLOCKS
        # If Server A locks [User1, User2] and Server B locks [User2, User1],
        # they will freeze forever waiting for each other.
        #
        # The fix is NOT sorting the Python list — IN (...) does not respect
        # list order, Postgres is free to return matching rows in whatever
        # order its own query plan produces, and that can differ between
        # concurrent transactions even for an identical query.
        #
        # The actual guarantee comes from ORDER BY: Postgres acquires
        # FOR UPDATE locks in the order rows are returned, so ordering by a
        # stable column (id) means every transaction that touches these two
        # accounts locks them in the same order, every time — no circular
        # wait, no deadlock.
        # -------------------------------------------------------------------
        account_ids = [payment_data.user_account_id, payment_data.merchant_account_id]

        accounts = (
            db.query(models.Account)
            .filter(models.Account.id.in_(account_ids))
            .order_by(models.Account.id)
            .with_for_update()
            .all()
        )
        
        # Map the results so we know which is which
        account_map = {str(acc.id): acc for acc in accounts}
        user_account = account_map.get(str(payment_data.user_account_id))
        merchant_account = account_map.get(str(payment_data.merchant_account_id))
        
        # -------------------------------------------------------------------
        # 3. VALIDATION
        # -------------------------------------------------------------------
        if not user_account or not merchant_account:
            raise ValueError("One or both accounts do not exist.")
            
        # Because of our lock, this balance check is 100% accurate and safe.
        if user_account.balance < payment_data.amount_cents:
            raise ValueError("Insufficient funds.")

        # -------------------------------------------------------------------
        # 4. MUTATE BALANCES (In Memory)
        # -------------------------------------------------------------------
        user_account.balance -= payment_data.amount_cents
        merchant_account.balance += payment_data.amount_cents

        # -------------------------------------------------------------------
        # 5. CREATE THE TRANSACTION RECORD
        # We MUST pass id=payment_data.transaction_id explicitly here.
        # Without it, SQLAlchemy's default=uuid.uuid4 on Transaction.id
        # would silently generate a DIFFERENT id than the one already
        # returned to the client in the /charge response — leaving the
        # client holding a tracking number that never exists in the DB.
        # -------------------------------------------------------------------
        new_txn = models.Transaction(
            id=payment_data.transaction_id,
            idempotency_key=payment_data.idempotency_key,
            description=payment_data.description,
            status=models.TransactionStatus.COMPLETED,
            amount_cents=payment_data.amount_cents,
        )
        db.add(new_txn)
        
        # db.flush() sends the SQL to the database to generate the new_txn.id, 
        # but does NOT commit it permanently yet. We need the ID for the lines.
        db.flush() 

        # -------------------------------------------------------------------
        # 6. DOUBLE-ENTRY BOOKKEEPING (The Zero-Sum Lines)
        # -------------------------------------------------------------------
        debit_line = models.LedgerLine(
            transaction_id=new_txn.id,
            account_id=user_account.id,
            amount=-payment_data.amount_cents # Negative (Money leaving)
        )
        credit_line = models.LedgerLine(
            transaction_id=new_txn.id,
            account_id=merchant_account.id,
            amount=payment_data.amount_cents  # Positive (Money arriving)
        )
        db.add_all([debit_line, credit_line])

        # -------------------------------------------------------------------
        # 7. COMMIT & RELEASE LOCKS
        # -------------------------------------------------------------------
        # This saves everything atomically. If the power goes out right now, 
        # the transaction is safely recorded. The Row Locks are released.
        db.commit()
        return new_txn

    except DuplicateTransactionError:
        # Nothing was written yet (we caught this before step 1), but keep
        # the rollback for symmetry / in case the session has other pending
        # state from the caller.
        db.rollback()
        raise

    except IntegrityError as e:
        # BACKSTOP for the race condition our proactive check above can't
        # cover: two workers both pass the SELECT check (neither has
        # committed yet) and both try to insert. Postgres's unique
        # constraint on idempotency_key guarantees only one insert wins;
        # the loser lands here. We treat it the same as a normal duplicate.
        db.rollback()
        if "idempotency_key" in str(e.orig):
            # Look up the row the OTHER transaction just committed, so we
            # can report its actual status rather than guessing.
            winner = db.query(models.Transaction).filter_by(
                idempotency_key=payment_data.idempotency_key
            ).first()
            raise DuplicateTransactionError(
                f"Transaction with idempotency_key={payment_data.idempotency_key} "
                f"was inserted concurrently by another worker "
                f"(status={winner.status.value if winner else 'unknown'}).",
                status=winner.status if winner else None,
            ) from e
        # Some other constraint was violated (not the one we expect) —
        # don't misreport it as a duplicate, let it surface as-is.
        raise

    except ValueError as e:
        # BUSINESS RULE FAILURE (insufficient funds, missing account, etc.)
        # We must NOT let this vanish silently. Two things need to happen,
        # and they need OPPOSITE treatment from the same session:
        #   1. The balance mutations we made earlier in this function must
        #      be undone — db.rollback() does that.
        #   2. A FAILED Transaction record must be KEPT, as the audit trail
        #      for the rejection.
        # rollback() is all-or-nothing for whatever is pending in the
        # session at the moment it runs — it can't selectively keep one
        # change and discard another. So we don't try to save the FAILED
        # row in the same breath as the mutations we're discarding. We
        # roll back FIRST (cleaning the session completely), and only
        # THEN, as a fresh, independent unit of work, create and commit
        # the failure record. By the time we add() it, rollback() has
        # already finished — there's nothing left for it to undo.
        db.rollback()

        failed_txn = models.Transaction(
            id=payment_data.transaction_id,
            idempotency_key=payment_data.idempotency_key,
            description=payment_data.description,
            status=models.TransactionStatus.FAILED,
            amount_cents=payment_data.amount_cents,
        )
        db.add(failed_txn)
        try:
            db.commit()
        except IntegrityError as commit_err:
            # Same race we already handle above (see the outer
            # `except IntegrityError`), but happening on OUR OWN write
            # this time — another worker, processing the same
            # idempotency_key concurrently, committed its own
            # Transaction row (FAILED or COMPLETED) a moment before we
            # did. Our sibling `except IntegrityError` clause above
            # cannot catch this: Python does not let one except block
            # hand off to another except block on the same try — this
            # error happened INSIDE except ValueError, so it needs its
            # own handling, right here.
            db.rollback()
            winner = db.query(models.Transaction).filter_by(
                idempotency_key=payment_data.idempotency_key
            ).first()
            raise DuplicateTransactionError(
                f"Transaction with idempotency_key={payment_data.idempotency_key} "
                f"was already resolved concurrently by another worker "
                f"(status={winner.status.value if winner else 'unknown'}) "
                f"while we were recording our own failure.",
                status=winner.status if winner else None,
            ) from commit_err

        raise

    except Exception as e:
        # IF ANYTHING ELSE FAILS (e.g., a DB connection drop, a lock
        # timeout), ROLLBACK the entire transaction. Undo balance changes.
        # Release the locks. This is a SYSTEM error, not a business
        # rejection — we do NOT write a FAILED record here, because we
        # don't actually know the payment was rejected; it may not have
        # been attempted at all, and the worker will NACK this so Pub/Sub
        # retries it later. Writing FAILED here would be a lie.
        db.rollback()
        raise e