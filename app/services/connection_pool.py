import threading
import time
from typing import Optional, List, Dict, Any
from pyhive import presto
import app.config.settings as settings
import socket
import requests


class PrestoConnectionPool:
    """
    A connection pool for Presto/Trino database connections.

    This class implements a thread-safe connection pool that manages
    the lifecycle of database connections, including creation, validation,
    and recycling of connections.
    """

    connect_kwargs: Dict[str, Any] = {}
    is_valid = False

    def __init__(self, min_conn: int = 2, max_conn: int = 10,
                 max_idle_time: int = 600, validate_interval: int = 30):
        """
        Initialize the connection pool.

        Args:
            min_conn: Minimum number of connections to keep in the pool
            max_conn: Maximum number of connections allowed in the pool
            max_idle_time: Maximum idle time for a connection in seconds before it's closed
            validate_interval: Time interval in seconds to validate idle connections
        """
        self._pool: List[Dict[str, Any]] = []  # Available connections
        # Connections currently in use
        self._in_use: Dict[int, Dict[str, Any]] = {}
        self._min_conn: int = min_conn
        self._max_conn: int = max_conn
        self._max_idle_time: int = max_idle_time
        self._validate_interval: int = validate_interval
        self._lock = threading.RLock()
        self._last_validation_time = 0

        if not settings.PRESTO_USERNAME or not settings.PRESTO_PASSWORD:
            raise ValueError("Presto username and password are required")

        # Create requests session with proper timeout settings
        requests_session = self._create_requests_session()

        # Prepare connection arguments
        self.connect_kwargs = {
            "host": settings.PRESTO_HOST,
            "port": settings.PRESTO_PORT,
            "protocol": settings.PRESTO_PROTOCOL,
            "username": settings.PRESTO_USERNAME,
            "password": settings.PRESTO_PASSWORD,
            "schema": settings.PRESTO_SCHEMA,
            "requests_session": requests_session
        }

        # Add source setting
        if settings.PRESTO_SOURCE:
            self.connect_kwargs["source"] = settings.PRESTO_SOURCE.replace(
                "username", settings.PRESTO_USERNAME)

        # Add catalog setting
        if settings.PRESTO_CATALOG:
            self.connect_kwargs["catalog"] = settings.PRESTO_CATALOG

        # Check host connectivity first
        self._check_host_connectivity(settings.PRESTO_HOST, int(
            settings.PRESTO_PORT), settings.CONNECT_TIMEOUT)

        # Initialize the pool with minimum connections
        self._initialize_pool()

        # Start validation thread
        self._stop_validation = False
        self._validation_thread = threading.Thread(
            target=self._validation_worker, daemon=True)
        self._validation_thread.start()

    def _initialize_pool(self) -> None:
        """Initialize the pool with the minimum number of connections."""

        for _ in range(self._min_conn):
            try:
                conn = self._create_new_connection(self.connect_kwargs)
                if not self.is_valid:
                    # Check if the connection is valid
                    self.is_valid = self.verify_connection(conn)

                self._pool.append({
                    'conn': conn,
                    'created_time': time.time(),
                    'last_used_time': time.time()
                })
            except Exception as e:
                print(f"Error initializing connection in pool: {str(e)}")
                # Continue trying to initialize other connections
                continue

    def _validation_worker(self) -> None:
        """Background worker to validate connections and maintain pool size."""
        while not self._stop_validation:
            try:
                self._validate_connections()
                time.sleep(self._validate_interval)
            except Exception as e:
                print(f"Error in validation worker: {str(e)}")
                time.sleep(5)  # Sleep briefly before retrying

    def _validate_connections(self) -> None:
        """Validate idle connections in the pool and remove stale ones."""
        current_time = time.time()

        # Only validate at configured intervals
        if current_time - self._last_validation_time < self._validate_interval:
            return

        self._last_validation_time = current_time

        with self._lock:
            # Check idle connections
            valid_connections = []
            for conn_info in self._pool:
                conn = conn_info['conn']
                last_used_time = conn_info['last_used_time']

                # Check if connection has been idle for too long
                if current_time - last_used_time > self._max_idle_time:
                    try:
                        conn.close()
                        print(f"Closed idle connection that exceeded max idle time")
                    except Exception as e:
                        print(f"Error closing idle connection: {str(e)}")
                    continue

                # Validate connection is still working
                if self.is_valid or self.verify_connection(conn):
                    valid_connections.append(conn_info)
                else:
                    try:
                        conn.close()
                        print(f"Closed invalid connection during validation")
                    except Exception as e:
                        print(f"Error closing invalid connection: {str(e)}")

            # Update pool with valid connections
            self._pool = valid_connections

            # Create new connections if below minimum
            while len(self._pool) + len(self._in_use) < self._min_conn:
                try:
                    conn = self._create_new_connection(self.connect_kwargs)
                    self._pool.append({
                        'conn': conn,
                        'created_time': time.time(),
                        'last_used_time': time.time()
                    })
                    print(f"Added new connection to maintain minimum pool size")
                except Exception as e:
                    print(
                        f"Error creating new connection during validation: {str(e)}")
                    break

    def _check_host_connectivity(self, host: str, port: int, timeout: int) -> bool:
        """Check if host port is accessible, fail fast"""
        start_time = time.time()
        sock = None
        try:
            # Try TCP connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))
            elapsed = time.time() - start_time
            print(f"✅ Host connectivity check succeeded in {elapsed:.2f}s")
            return True
        except Exception as e:
            elapsed = time.time() - start_time
            print(
                f"Cannot connect to {host}:{port} ({elapsed:.2f}s): {str(e)}")
            return False
        finally:
            if sock:
                sock.close()

    def _create_new_connection(self, connect_kwargs: Dict[str, Any]) -> presto.Connection:
        """Create a new Presto connection."""
        # Create connection
        conn = presto.connect(**connect_kwargs)
        return conn

    def _create_requests_session(self):
        """Create a requests session with timeout configuration"""
        session = requests.Session()

        # Create adapter based on configured protocol
        protocol = settings.PRESTO_PROTOCOL.lower()
        adapter = requests.adapters.HTTPAdapter(max_retries=1)

        # Only mount the required protocol
        if protocol == 'https':
            session.mount('https://', adapter)
            print(f"✅ Created session with HTTPS adapter")
        elif protocol == 'http':
            session.mount('http://', adapter)
            print(f"✅ Created session with HTTP adapter")
        else:
            # For safety, if protocol is unclear, mount both
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            print(
                f"⚠️ Unknown protocol '{protocol}', mounting both HTTP and HTTPS adapters")

        # Add timeout defaults by modifying requests.Session.request method
        original_request = session.request

        def request_with_timeout(*args, **kwargs):
            # If timeout is not specified, add default timeout
            if 'timeout' not in kwargs:
                # Use (connect_timeout, read_timeout) format
                kwargs['timeout'] = (
                    settings.CONNECT_TIMEOUT, settings.QUERY_TIMEOUT)
            return original_request(*args, **kwargs)

        # Replace the original request method
        session.request = request_with_timeout
        return session

    def verify_connection(self, conn) -> bool:
        """Verify that the connection to Presto is successful."""
        cursor = conn.cursor()
        try:
            # Execute a simple query to verify connection
            start_time = time.time()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            elapsed = time.time() - start_time
            if result and result[0] == 1:
                print(
                    f"✅ Presto connection verified successfully in {elapsed:.2f}s!")
                return True
            else:
                print(
                    f"⚠️ Warning: Presto connection test returned unexpected result ({elapsed:.2f}s):", result)
                return False
        except Exception as e:
            # If connection fails, close connection and raise exception
            elapsed = time.time() - start_time
            cursor.close()
            print(f"Failed to connect to Presto ({elapsed:.2f}s): {str(e)}")
            return False
        finally:
            cursor.close()

    def get_connection(self) -> presto.Connection:
        """
        Get a connection from the pool.

        Returns:
            A Presto connection object

        Raises:
            ValueError: If cannot get a valid connection
        """
        with self._lock:
            # First, try to get an existing connection from the pool
            if self._pool:
                conn_info = self._pool.pop()
                conn = conn_info['conn']
                conn_info['last_used_time'] = time.time()
                self._in_use[id(conn)] = conn_info
                return conn

            # If no valid connection in pool, check if we can create a new one
            if len(self._in_use) < self._max_conn:
                try:
                    conn = self._create_new_connection(self.connect_kwargs)
                    conn_info = {
                        'conn': conn,
                        'created_time': time.time(),
                        'last_used_time': time.time()
                    }
                    self._in_use[id(conn)] = conn_info
                    return conn
                except Exception as e:
                    raise ValueError(
                        f"Failed to create new connection: {str(e)}")

            # Pool exhausted
            print(
                f"Connection pool exhausted, max connections: {self._max_conn}. Just wait for a moment...")

    def release_connection(self, conn: presto.Connection) -> None:
        """
        Release a connection back to the pool.

        Args:
            conn: The connection to release
        """
        if not conn:
            return

        with self._lock:
            conn_id = id(conn)
            if conn_id in self._in_use:
                conn_info = self._in_use.pop(conn_id)
                conn_info['last_used_time'] = time.time()
                # Return to pool if we're under max capacity
                if len(self._pool) < self._max_conn:
                    self._pool.append(conn_info)
                    return

                # If we're at capacity or connection is invalid, close it
                try:
                    conn.close()
                except Exception as e:
                    print(f"Error closing connection: {str(e)}")

    def close_all(self) -> None:
        """Close all connections in the pool."""
        self._stop_validation = True

        if self._validation_thread.is_alive():
            self._validation_thread.join(timeout=1.0)

        with self._lock:
            # Close in-use connections
            for conn_info in self._in_use.values():
                try:
                    conn_info['conn'].close()
                except Exception as e:
                    print(f"Error closing in-use connection: {str(e)}")

            # Close pooled connections
            for conn_info in self._pool:
                try:
                    conn_info['conn'].close()
                except Exception as e:
                    print(f"Error closing pooled connection: {str(e)}")

            # Clear the collections
            self._in_use.clear()
            self._pool.clear()

    def __del__(self):
        """Destructor to ensure connections are closed."""
        try:
            self.close_all()
        except:
            pass


# Singleton instance of the connection pool
_pool_instance: Optional[PrestoConnectionPool] = None
_pool_lock = threading.Lock()


def get_connection_pool(min_conn: int = 2, max_conn: int = 10) -> PrestoConnectionPool:
    """
    Get the global connection pool instance.

    Args:
        min_conn: Minimum number of connections to keep
        max_conn: Maximum number of connections allowed

    Returns:
        The connection pool instance
    """
    global _pool_instance

    if _pool_instance is None:
        with _pool_lock:
            if _pool_instance is None:
                _pool_instance = PrestoConnectionPool(
                    min_conn=min_conn, max_conn=max_conn)

    return _pool_instance
