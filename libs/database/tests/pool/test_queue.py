import threading
import time
from unittest.mock import MagicMock

from libs.database.pool import QueueConnectionPool


class TestQueueConnectionPool:
    """Unit tests for the QueueConnectionPool connection management strategy."""

    def test_init_default_size(self):
        """
        GIVEN a QueueConnectionPool initialized without a size
        THEN it should default to a size of 5
        WHEN the pool is created
        """
        connector = MagicMock()
        pool = QueueConnectionPool(connector=connector)
        assert pool._queue.maxsize == 5

    def test_init_custom_size(self):
        """
        GIVEN a QueueConnectionPool initialized with a custom size
        THEN it should use the specified size
        WHEN the pool is created
        """
        connector = MagicMock()
        pool = QueueConnectionPool(connector=connector, size=3)
        assert pool._queue.maxsize == 3

    def test_lease_lazy_initialization(self):
        """
        GIVEN an empty QueueConnectionPool
        THEN the connector should be called to create a new connection
        WHEN a connection is leased
        """
        mock_conn = MagicMock()
        connector = MagicMock(return_value=mock_conn)
        pool = QueueConnectionPool(connector=connector, size=1)

        with pool.lease() as conn:
            assert conn == mock_conn
        connector.assert_called_once()
        assert pool._current_size == 1

    def test_lease_reuses_connection(self):
        """
        GIVEN a QueueConnectionPool with an available connection
        THEN the existing connection should be reused
        WHEN a connection is leased
        """
        mock_conn = MagicMock()
        connector = MagicMock(return_value=mock_conn)
        pool = QueueConnectionPool(connector=connector, size=1)

        with pool.lease():  # Creates and puts back
            pass
        connector.assert_called_once()

        with pool.lease() as conn:  # Reuses
            assert conn == mock_conn
        connector.assert_called_once()  # Still only once

    def test_lease_queue_full_closes_connection(self):
        """
        GIVEN a QueueConnectionPool where the queue is full
        THEN the connection should be closed when returned
        WHEN a connection is leased and returned
        """
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        connector = MagicMock(side_effect=[mock_conn1, mock_conn2])
        pool = QueueConnectionPool(connector=connector, size=1)

        # Lease 1: Creates mock_conn1, puts it back
        with pool.lease():
            pass

        # Lease 2: Creates mock_conn2 (queue is full, so it won't be put back)
        with pool.lease():
            pass

        # mock_conn2 should have been closed because it couldn't be put back
        mock_conn2.close.assert_called_once()
        assert pool._current_size == 1  # Only mock_conn1 remains in the pool

    def test_close_all_clears_connections(self):
        """
        GIVEN a QueueConnectionPool with active connections
        THEN all connections should be closed and the queue emptied
        WHEN close_all is invoked
        """
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        connector = MagicMock(side_effect=[mock_conn1, mock_conn2])
        pool = QueueConnectionPool(connector=connector, size=2)

        with pool.lease():  # conn1
            pass
        with pool.lease():  # conn2
            pass

        assert pool._current_size == 2
        assert pool._queue.qsize() == 2

        pool.close_all()

        assert pool._current_size == 0
        assert pool._queue.empty()
        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_called_once()

    def test_lease_thread_safety(self):
        """
        GIVEN a QueueConnectionPool accessed by multiple threads
        THEN it should manage connections safely without deadlocks or errors
        WHEN concurrent leases are attempted
        """
        connector_calls = threading.Semaphore(0)

        def mock_connector():
            connector_calls.release()
            return MagicMock()

        pool = QueueConnectionPool(connector=mock_connector, size=3)

        def worker():
            with pool.lease():
                time.sleep(0.01)  # Simulate work

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify that at most 'size' connections were created
        assert connector_calls._value <= pool._queue.maxsize
        # Verify that all connections were eventually returned to the pool
        assert pool._queue.qsize() == pool._queue.maxsize
