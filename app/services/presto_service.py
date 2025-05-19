import pandas as pd
from pyhive import presto
from typing import List, Dict, Any, Tuple, Optional
import app.config.settings as settings


class PrestoService:
    @staticmethod
    def create_connection():
        """Create a connection to Presto."""
        if not settings.PRESTO_USERNAME or not settings.PRESTO_PASSWORD:
            raise ValueError("Presto username and password are required")
            
        conn = presto.connect(
            host=settings.PRESTO_HOST,
            port=settings.PRESTO_PORT,
            protocol=settings.PRESTO_PROTOCOL,
            username=settings.PRESTO_USERNAME,
            password=settings.PRESTO_PASSWORD,
            schema=settings.PRESTO_SCHEMA,
            source=settings.PRESTO_SOURCE.replace("username", settings.PRESTO_USERNAME) if settings.PRESTO_SOURCE else None
        )
        return conn

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
        conn = PrestoService.create_connection()
        cursor = conn.cursor()
        
        try:
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
                
                return columns, data, row_count
            else:
                # For queries that don't return results (e.g., INSERT, UPDATE)
                return [], [], 0
        finally:
            cursor.close()
            conn.close()
    
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
        conn = PrestoService.create_connection()
        cursor = conn.cursor()
        
        try:
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
                
                return df
            else:
                # For queries that don't return results
                return pd.DataFrame()
        finally:
            cursor.close()
            conn.close() 