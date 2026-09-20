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