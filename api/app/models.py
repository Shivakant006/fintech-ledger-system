from sqlalchemy import Column, String, BigInteger, ForeignKey, DateTime, Enum, CheckConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
import datetime
import enum
from .database import Base

# We use strict Enums to prevent invalid data (like a typo in "PENDING")
class TransactionStatus(enum.Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"

class AccountType(enum.Enum):
    USER = "USER"
    MERCHANT = "MERCHANT"
    SYSTEM_FEE = "SYSTEM_FEE"

class Account(Base):
    __tablename__ = "accounts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    type = Column(Enum(AccountType), nullable=False)
    # Balances are strictly in CENTS (BigInteger) to prevent float precision errors
    balance = Column(BigInteger, default=0, nullable=False) 
    currency = Column(String(3), default="INR")

    # DB-level constraint: A user's balance can never drop below zero.
    __table_args__ = (
        CheckConstraint(
            "(type = 'USER' AND balance >= 0) OR type != 'USER'",
            name="check_positive_user_balance"
        ),
    )

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # The idempotency key ensures the client can retry safely without double-charging
    idempotency_key = Column(String(255), unique=True, nullable=False, index=True)
    description = Column(String(255))
    status = Column(Enum(TransactionStatus), default=TransactionStatus.PENDING)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    # The requested payment amount, in cents. Stored directly on the
    # Transaction (not just derived from LedgerLine) because a FAILED
    # transaction has NO LedgerLine rows at all — there was no money
    # movement to record — but a client still needs to know how much
    # they attempted to send. nullable=False because every code path
    # that creates a Transaction (success or failure) already has this
    # value in scope from the original PaymentTask; there's no
    # legitimate case where it would be unknown.
    amount_cents = Column(BigInteger, nullable=False)

    # This allows us to easily fetch all lines associated with this transaction via Python
    lines = relationship("LedgerLine", back_populates="transaction")

class LedgerLine(Base):
    __tablename__ = "ledger_lines"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    transaction_id = Column(UUID(as_uuid=True), ForeignKey("transactions.id"), nullable=False)
    account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.id"), nullable=False)
    amount = Column(BigInteger, nullable=False) # Positive = Credit, Negative = Debit
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    transaction = relationship("Transaction", back_populates="lines")