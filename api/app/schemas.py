from pydantic import BaseModel, Field, UUID4
from typing import Optional
from uuid import UUID

class PaymentRequest(BaseModel):
    # The client must provide a unique key for this specific action
    idempotency_key: str = Field(..., description="Unique key to prevent duplicate charges")
    
    # Who is paying, and who is receiving?
    user_account_id: UUID
    merchant_account_id: UUID
    
    # Money is in CENTS. We enforce > 0 so people can't send negative money!
    amount_cents: int = Field(..., gt=0, description="Amount in cents (e.g., 500 for $5.00)")
    description: Optional[str] = "Payment"

class PaymentResponse(BaseModel):
    transaction_id: UUID
    status: str
    message: str