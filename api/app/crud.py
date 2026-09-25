from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from . import models, schemas


class DuplicateTransactionError(Exception):
    """Raised when a transaction with this idempotency_key has already been
    settled. This is not a failure — it means an earlier attempt (or a
    redelivered Pub/Sub message for the same attempt) already succeeded.
    The caller should treat this as a no-op, not a retryable error."""
    pass


def execute_payment(db: Session, payment_data: schemas.PaymentTask) -> models.Transaction:
    # A try/except block is mandatory. If any step fails, we MUST rollback.
    try:
        # -------------------------------------------------------------------
        # 0. IDEMPOTENCY CHECK (proactive path)
        # Pub/Sub is at-least-once delivery: the same message can reach us
        # more than once. If we've already settled this idempotency_key,
        # this is a redelivery, not a new payment. Handle it here, before
        # we touch any account rows.
        # -------------------------------------------------------------------
        existing = db.query(models.Transaction).filter_by(
            idempotency_key=payment_data.idempotency_key
        ).first()
        if existing:
            raise DuplicateTransactionError(
                f"Transaction with idempotency_key={payment_data.idempotency_key} "
                f"already exists (id={existing.id})."
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
        # -------------------------------------------------------------------
        new_txn = models.Transaction(
            idempotency_key=payment_data.idempotency_key,
            description=payment_data.description,
            status=models.TransactionStatus.COMPLETED
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
        # the transaction is safely recorded. The Row Locks are released.
        db.commit()
        return new_txn

    except Exception as e:
        # IF ANYTHING FAILS (e.g., database constraint violation, or our ValueError),
        # ROLLBACK the entire transaction. Undo balance changes. Release the locks.
        db.rollback()
        raise e
