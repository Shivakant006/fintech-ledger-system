from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from . import models, schemas

def execute_payment(db: Session, payment_data: schemas.PaymentRequest) -> models.Transaction:
    # A try/except block is mandatory. If any step fails, we MUST rollback.
    try:
        # -------------------------------------------------------------------
        # 1. PREVENT DEADLOCKS (The Senior Engineer Secret)
        # If Server A locks [User1, User2] and Server B locks [User2, User1], 
        # they will freeze forever waiting for each other. 
        # We solve this by ALWAYS locking rows in a consistent order (alphabetical by UUID).
        # -------------------------------------------------------------------
        account_ids = sorted([
            str(payment_data.user_account_id), 
            str(payment_data.merchant_account_id)
        ])
        
        # -------------------------------------------------------------------
        # 2. THE ROW LOCK (SELECT FOR UPDATE)
        # Fetch both accounts and lock them. Other servers will pause here.
        # -------------------------------------------------------------------
        accounts = db.query(models.Account).filter(
            models.Account.id.in_(account_ids)
        ).with_for_update().all()
        
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
        # This saves everything atomically. If the power goes out right now, 
        # the transaction is safely recorded. The Row Locks are released.
        db.commit()
        return new_txn

    except Exception as e:
        # IF ANYTHING FAILS (e.g., database constraint violation, or our ValueError),
        # ROLLBACK the entire transaction. Undo balance changes. Release the locks.
        db.rollback()
        raise e