from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
import os

# In production, this URL comes from Secret Manager and points to Cloud SQL.
# For local dev, we use a local PostgreSQL instance.
SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://postgres:password@localhost:5432/ledger_db"
)

# engine is the core interface to the database.
# pool_size and max_overflow are crucial for FinTech high-throughput!
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=20,          # Keep 20 connections open and ready
    max_overflow=10,       # Allow 10 extra connections during traffic spikes
)

# SessionLocal is a factory that gives us a database session for each API request.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# All our models will inherit from this Base class.
Base = declarative_base()


def get_db():
    """
    FastAPI dependency: opens one SessionLocal() per request, hands it to
    the endpoint via Depends(get_db), and guarantees it's closed afterward
    — success or failure — via the try/finally. This is the standard
    FastAPI pattern for request-scoped database access.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()