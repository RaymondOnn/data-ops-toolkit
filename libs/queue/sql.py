# from datetime import datetime, timedelta, UTC
# from sqlalchemy import text

# class AdvancedSQLQueue:
#     def __init__(self, engine, max_retries=3, visibility_timeout=30):
#         self.engine = engine
#         self.max_retries = max_retries
#         self.visibility_timeout = visibility_timeout # seconds

#     def dequeue(self):
#         """
#         Handles:
#         - Visibility Timeout: Only picks jobs where available_at <= NOW.
#         - Redelivery: If a worker crashed, available_at will eventually pass.
#         - Ordering: ORDER BY created_at.
#         """
#         now = datetime.now(UTC)
        
#         # 1. Atomic Claim with SKIP LOCKED
#         # available_at <= now ensures we don't pick up retries too early (backoff)
#         # or jobs currently being processed by others (visibility timeout)
#         find_query = text("""
#             SELECT id FROM jobs 
#             WHERE status = 'pending' AND available_at <= :now
#             ORDER BY created_at ASC 
#             LIMIT 1 FOR UPDATE SKIP LOCKED
#         """)

#         # 2. Update state immediately (The "Lease")
#         update_query = text("""
#             UPDATE jobs 
#             SET status = 'pending', 
#                 available_at = :next_visibility,
#                 retry_count = retry_count + 1
#             WHERE id = :id 
#             RETURNING *
#         """)

#         next_visibility = now + timedelta(seconds=self.visibility_timeout)

#         with self.engine.begin() as conn:
#             row = conn.execute(find_query, {"now": now}).fetchone()
#             if row:
#                 return conn.execute(update_query, {
#                     "id": row.id, 
#                     "next_visibility": next_visibility
#                 }).fetchone()
#         return None

#     def nack(self, job_id, retry_count):
#         """
#         Negative Acknowledgement (Retry with Exponential Backoff)
#         """
#         # Backoff: 2^retry_count * 10 seconds (e.g., 10s, 20s, 40s...)
#         backoff_seconds = (2 ** retry_count) * 10
#         next_available = datetime.now(UTC) + timedelta(seconds=backoff_seconds)
        
#         status = "pending" if retry_count < self.max_retries else "dlq"
        
#         query = text("""
#             UPDATE jobs 
#             SET status = :status, available_at = :available_at 
#             WHERE id = :id
#         """)
#         with self.engine.begin() as conn:
#             conn.execute(query, {
#                 "status": status, 
#                 "available_at": next_available, 
#                 "id": job_id
#             })