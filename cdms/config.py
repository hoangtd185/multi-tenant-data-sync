import os
import socket

DSN = os.getenv("CDMS_DSN", "postgresql://postgres@127.0.0.1:5432/cdms")
REDIS_URL = os.getenv("CDMS_REDIS_URL", "redis://127.0.0.1:6379/0")

HEARTBEAT_TIMEOUT_S = int(os.getenv("CDMS_HEARTBEAT_TIMEOUT_S", "30"))
LEASE_S = int(os.getenv("CDMS_LEASE_S", "30"))
BATCH_SIZE = int(os.getenv("CDMS_BATCH_SIZE", "200"))

NODE_ID = os.getenv("CDMS_NODE_ID", f"{socket.gethostname()}-{os.getpid()}")
