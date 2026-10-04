from pydantic import BaseModel, Field, UUID4
from typing import Optional
from uuid import UUID
import datetime

class PaymentRequest(BaseModel):
    # The client must provide a unique key for this specific action
    idempotency_key: str = Field(..., description="Unique key to prevent duplicate charges")
    
    # Who is paying, and who is receiving?
    user_account_id: UUID
    merchant_account_id: UUID
    
    # Money is in CENTS. We enforce > 0 so people can't send negative money!
    amount_cents: int = Field(..., gt=0, description="Amount in cents (e.g., 500 for $5.00)")
    description: Optional[str] = "Payment"

class PaymentTask(PaymentRequest):
    """
    Internal-only schema used for the Pub/Sub message payload.

    This is everything a client-submitted PaymentRequest has, PLUS the
    transaction_id the API generates right after validating the request.
    It is never used as the shape of an incoming HTTP request — a client
    doesn't have a transaction_id yet when they call /charge, the server
    assigns one. This is what lets the worker create the Transaction row
    using the SAME id the client was already handed as their tracking
    number, instead of the database silently minting a different one.
    """
    transaction_id: UUID

class PaymentResponse(BaseModel):
    transaction_id: UUID
    status: str
    message: str

class TransactionStatusResponse(BaseModel):
    """
    Response shape for GET /transactions/{transaction_id}. Deliberately
    does NOT include LedgerLine detail (account ids, individual debit/
    credit rows) — that's internal bookkeeping. A client checking on
    their payment needs to know: did it go through, and for how much.
    """
    transaction_id: UUID
    status: str
    amount_cents: int
    description: Optional[str]
    created_at: datetime.datetime
    # Built explicitly in the endpoint, not via from_attributes/ORM mode:
    # the Transaction model's primary key is .id (not .transaction_id),
    # and .status is a TransactionStatus ENUM object, not a plain string
    # — an automatic attribute-name mapping would silently mismatch the
    # first and hand back the wrong type for the second.