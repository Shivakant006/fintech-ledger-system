import redis
import os

# In GCP, this will point to Cloud Memorystore
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Create a connection pool for Redis (similar to what we did for Postgres)
redis_pool = redis.ConnectionPool(
    host=REDIS_HOST, 
    port=REDIS_PORT, 
    db=0, 
    decode_responses=True # Automatically decodes bytes to strings
)

def get_redis():
    return redis.Redis(connection_pool=redis_pool)

def ping_redis(host: str = REDIS_HOST, port: int = REDIS_PORT, timeout: float = 1.0) -> bool:
    """
    Health-check helper: a short-lived client with tight timeouts, kept
    separate from the shared pool above so a hung Redis can't make a health
    probe hang. Raises on failure; returns True if Redis answers PING.
    """
    client = redis.Redis(
        host=host, port=port, db=0,
        socket_connect_timeout=timeout, socket_timeout=timeout,
    )
    try:
        return client.ping()
    finally:
        client.close()