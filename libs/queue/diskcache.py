import time
import uuid
from diskcache import Deque, Cache

class DiskcacheQueue:
    def __init__(self, directory, visibility_timeout=30, max_retries=3):
        # We use a primary Cache for the metadata/state
        self.cache = Cache(directory)
        self.visibility_timeout = visibility_timeout
        self.max_retries = max_retries
        
        # We use a Deque for the actual Kafka-style ordering
        self.queue = Deque(directory=f"{directory}/queue")

    def enqueue(self, payload, group_id="default"):
        """Adds a message to the disk-backed deque."""
        job_id = str(uuid.uuid4())
        message = {
            "id": job_id,
            "payload": payload,
            "group_id": group_id,
            "retry_count": 0,
            "available_at": time.time(),
            "status": "pending"
        }
        # Metadata stored in cache, ID pushed to queue
        self.cache[job_id] = message
        self.queue.append(job_id)

    def dequeue(self):
        """
        Implements:
        - Visibility Timeout
        - Redelivery
        - Competing Consumers
        """
        # Iterate through the deque to find the first available message
        # In a real high-scale system, you'd prune the deque
        for _ in range(len(self.queue)):
            job_id = self.queue.popleft()
            
            with self.cache.transact():
                msg = self.cache.get(job_id)
                
                if not msg or msg["status"] == "dlq":
                    continue
                
                now = time.time()
                # Check if job is ready to be processed (handles backoff/timeout)
                if msg["available_at"] <= now:
                    msg["status"] = "processing"
                    msg["available_at"] = now + self.visibility_timeout
                    msg["retry_count"] += 1
                    
                    self.cache[job_id] = msg
                    # Re-add to the end of the queue so it can be 'redelivered' 
                    # if this worker crashes
                    self.queue.append(job_id)
                    return msg
                else:
                    # Not ready yet, put it back in the queue
                    self.queue.append(job_id)
            
            # Short sleep to prevent CPU spinning if queue is busy but locked
            time.sleep(0.01)
        return None

    def ack(self, job_id):
        """Job finished. Permanent removal."""
        with self.cache.transact():
            del self.cache[job_id]
            # The job_id will eventually be popped and ignored by dequeue

    def nack(self, job_id):
        """Negative Ack with Exponential Backoff."""
        with self.cache.transact():
            msg = self.cache.get(job_id)
            if not msg: return

            if msg["retry_count"] >= self.max_retries:
                msg["status"] = "dlq"
            else:
                msg["status"] = "pending"
                # Exponential backoff: 10s, 20s, 40s...
                wait = (2 ** msg["retry_count"]) * 10
                msg["available_at"] = time.time() + wait
            
            self.cache[job_id] = msg