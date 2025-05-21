import threading
import pandas as pd
from pyhive import presto
import requests
from typing import List, Dict, Any, Tuple, Optional
import app.config.settings as settings
import time
import socket


class PrestoService:
    _conn: Optional[presto.Connection] = None
    _conn_lock = threading.Lock()

    @staticmethod
    def get_connection():
        if PrestoService._conn_lock.acquire():
            if PrestoService._conn is None:
                PrestoService._conn = PrestoService.create_connection()
            PrestoService._conn_lock.release()
        return PrestoService._conn

    @staticmethod
    def create_connection():
        """Create a connection to Presto."""
        if not settings.PRESTO_USERNAME or not settings.PRESTO_PASSWORD:
            raise ValueError("Presto username and password are required")
            
        # 首先检查主机连通性
        PrestoService._check_host_connectivity(
            settings.PRESTO_HOST, 
            int(settings.PRESTO_PORT), 
            settings.CONNECT_TIMEOUT
        )
        
        # 创建自定义requests会话，配置超时
        requests_session = PrestoService._create_requests_session()
        
        # 准备连接参数
        connect_kwargs = {
            "host": settings.PRESTO_HOST,
            "port": settings.PRESTO_PORT,
            "protocol": settings.PRESTO_PROTOCOL,
            "username": settings.PRESTO_USERNAME,
            "password": settings.PRESTO_PASSWORD,
            "schema": settings.PRESTO_SCHEMA,
            # 自定义requests会话
            "requests_session": requests_session
        }
        
        # 添加source设置
        if settings.PRESTO_SOURCE:
            connect_kwargs["source"] = settings.PRESTO_SOURCE.replace("username", settings.PRESTO_USERNAME)
        
        # 添加catalog设置
        if settings.PRESTO_CATALOG:
            connect_kwargs["catalog"] = settings.PRESTO_CATALOG
            
        # Create connection
        conn = presto.connect(**connect_kwargs)
        
        # Set resource group after connection
        if settings.PRESTO_RESOURCE_GROUP:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SET SESSION resource_groups.id = '{settings.PRESTO_RESOURCE_GROUP}'")
                print(f"✅ Resource group set to: {settings.PRESTO_RESOURCE_GROUP}")
            except Exception as e:
                print(f"⚠️ Warning: Failed to set resource group: {str(e)}")
            finally:
                cursor.close()
        
        # Verify connection is successful
        PrestoService.verify_connection(conn)
        
        return conn
    
    @staticmethod
    def _check_host_connectivity(host, port, timeout):
        """Check if host port is accessible, fail fast"""
        start_time = time.time()
        try:
            # Try TCP connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((host, port))
            sock.close()
            elapsed = time.time() - start_time
            print(f"✅ Host connectivity check succeeded in {elapsed:.2f}s")
        except Exception as e:
            elapsed = time.time() - start_time
            raise ValueError(f"Cannot connect to {host}:{port} ({elapsed:.2f}s): {str(e)}")
    
    @staticmethod
    def _create_requests_session():
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
            print(f"⚠️ Unknown protocol '{protocol}', mounting both HTTP and HTTPS adapters")
        
        # Add timeout defaults by modifying requests.Session.request method
        original_request = session.request
        
        def request_with_timeout(*args, **kwargs):
            # If timeout is not specified, add default timeout
            if 'timeout' not in kwargs:
                # Use (connect_timeout, read_timeout) format
                kwargs['timeout'] = (settings.CONNECT_TIMEOUT, settings.QUERY_TIMEOUT)
            return original_request(*args, **kwargs)
        
        # Replace the original request method
        session.request = request_with_timeout
        
        return session
    
    @staticmethod
    def verify_connection(conn):
        """Verify that the connection to Presto is successful."""
        cursor = conn.cursor()
        try:
            # Execute a simple query to verify connection
            start_time = time.time()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            elapsed = time.time() - start_time
            if result and result[0] == 1:
                print(f"✅ Presto connection verified successfully in {elapsed:.2f}s!")
            else:
                print(f"⚠️ Warning: Presto connection test returned unexpected result ({elapsed:.2f}s):", result)
        except Exception as e:
            # If connection fails, close connection and raise exception
            elapsed = time.time() - start_time
            cursor.close()
            conn.close()
            raise ValueError(f"Failed to connect to Presto ({elapsed:.2f}s): {str(e)}")
        finally:
            cursor.close()

    @staticmethod
    def execute_query(query: str, params: Optional[Dict[str, Any]] = None, limit: Optional[int] = None) -> Tuple[List[str], List[List[Any]], int]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query to execute
            params: Query parameters (for parameterized queries)
            limit: Max number of rows to return
            
        Returns:
            Tuple of (column_names, data, row_count)
        """
        conn = PrestoService.get_connection()
        cursor = conn.cursor()
        
        try:
            # Set query timeout
            if settings.QUERY_TIMEOUT:
                try:
                    cursor.execute(f"SET SESSION query_max_execution_time = '{settings.QUERY_TIMEOUT}s'")
                except Exception as e:
                    print(f"⚠️ Warning: Failed to set query timeout: {str(e)}")
            
            print(f"Executing query with {settings.QUERY_TIMEOUT}s timeout: {query[:200]}{'...' if len(query) > 200 else ''}")
            start_time = time.time()
            
            # Execute the query with parameters if provided
            if params:
                # PyHive doesn't directly support parameterized queries,
                # but we could implement parameter substitution here if needed
                pass
                
            cursor.execute(query)
            
            # Get column names (if query returned results)
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch results with optional limit
                if limit:
                    results = cursor.fetchmany(limit)
                else:
                    results = cursor.fetchall()
                
                # Convert to list for JSON serializability
                data = [list(row) for row in results]
                row_count = len(data)
                
                elapsed = time.time() - start_time
                print(f"Query completed in {elapsed:.2f}s, returned {row_count} rows")
                
                return columns, data, row_count
            else:
                # For queries that don't return results (e.g., INSERT, UPDATE)
                elapsed = time.time() - start_time
                print(f"Query completed in {elapsed:.2f}s, no results returned")
                return [], [], 0
        finally:
            cursor.close()
    
    @staticmethod
    def execute_query_to_df(query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.
        
        Args:
            query: SQL query to execute
            params: Query parameters (for parameterized queries)
            
        Returns:
            pandas DataFrame with query results
        """
        conn = PrestoService.get_connection()
        cursor = conn.cursor()
        
        try:
            # Set query timeout
            if settings.QUERY_TIMEOUT:
                try:
                    cursor.execute(f"SET SESSION query_max_execution_time = '{settings.QUERY_TIMEOUT}s'")
                except Exception as e:
                    print(f"⚠️ Warning: Failed to set query timeout: {str(e)}")
            
            print(f"Executing query with {settings.QUERY_TIMEOUT}s timeout: {query[:200]}{'...' if len(query) > 200 else ''}")
            start_time = time.time()
            
            # Execute the query with parameters if provided
            if params:
                # PyHive doesn't directly support parameterized queries,
                # but we could implement parameter substitution here if needed
                pass
                
            cursor.execute(query)
            
            # Get column names
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch results
                results = cursor.fetchall()
                
                # Create DataFrame
                df = pd.DataFrame(results, columns=columns)
                
                elapsed = time.time() - start_time
                print(f"Query completed in {elapsed:.2f}s, returned {len(df)} rows")
                
                return df
            else:
                # For queries that don't return results
                elapsed = time.time() - start_time
                print(f"Query completed in {elapsed:.2f}s, no results returned")
                return pd.DataFrame()
        finally:
            cursor.close()
