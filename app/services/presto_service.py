import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
import app.config.settings as settings
import time
from app.services.connection_pool import get_connection_pool


class PrestoService:
    @staticmethod
    def get_connection():
        """Get a connection from the connection pool."""
        return get_connection_pool().get_connection()
    
    @staticmethod   
    def release_connection(conn):
        """Release a connection back to the connection pool."""
        get_connection_pool().release_connection(conn)

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
            PrestoService.release_connection(conn)
    
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
            PrestoService.release_connection(conn)


    @staticmethod
    def close_all():
        """Close all connections in the connection pool."""
        get_connection_pool().close_all()
