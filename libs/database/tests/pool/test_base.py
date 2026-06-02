import threading
from unittest.mock import MagicMock

from libs.database.pool.base import LockPool


class TestLockPool:
    """Unit tests for the LockPool connection management strategy."""

    def test_lease_lazy_initialization(self):
        """
        GIVEN a LockPool with a connector function
        THEN the connector should be called only once during the first lease
        WHEN multiple leases are requested sequentially
        """
        mock_conn = MagicMock()
        connector = MagicMock(return_value=mock_conn)
        pool = LockPool(connector=connector)

        # First lease triggers creation
        with pool.lease() as conn1:
            assert conn1 == mock_conn

        connector.assert_called_once()

        # Second lease reuses existing
        with pool.lease() as conn2:
            assert conn2 == mock_conn

        # Still only called once
        assert connector.call_count == 1

    def test_close_all_clears_connection(self):
        """
        GIVEN an active connection in the pool
        THEN the connection should be closed and the reference cleared
        WHEN close_all is invoked
        """
        mock_conn = MagicMock()
        pool = LockPool(connector=lambda: mock_conn)

        # Initialize the connection
        with pool.lease():
            pass

        assert pool._connection == mock_conn

        pool.close_all()

        assert pool._connection is None
        mock_conn.close.assert_called_once()

    def test_close_all_handles_no_close_method(self):
        """
        GIVEN a connection object without a close method
        THEN close_all should not raise an error and should clear the reference
        WHEN close_all is invoked
        """
        # A basic object that doesn't have a 'close' attribute
        mock_conn = object()
        pool = LockPool(connector=lambda: mock_conn)

        with pool.lease():
            pass

        # Should not raise AttributeError or Exception
        pool.close_all()
        assert pool._connection is None

    def test_lease_thread_safety(self):
        """
        GIVEN a LockPool accessed by multiple threads
        THEN only one connection should be created due to the internal lock
        WHEN concurrent leases are attempted
        """
        mock_conn = MagicMock()
        connector = MagicMock(return_value=mock_conn)
        pool = LockPool(connector=connector)

        def worker():
            with pool.lease():
                # Simulate some work
                import time

                time.sleep(0.01)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify atomicity: 10 threads, but only 1 connector call
        assert connector.call_count == 1
