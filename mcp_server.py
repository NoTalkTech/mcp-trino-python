#!/usr/bin/env python3

import asyncio
import json
import time
import pandas as pd
import traceback
import signal
import sys
from typing import Any, Dict, List, Optional

import mcp.types as types
from mcp.server import FastMCP

from app.services.presto_service import PrestoService
from app.config.settings import PRESTO_HOST, PRESTO_PORT, PRESTO_SCHEMA, print_config

# Create MCP server
app = FastMCP("mcp-trino-python")

# Register Trino resource
@app.resource(
    uri=f"trino://{PRESTO_HOST}:{PRESTO_PORT}/{PRESTO_SCHEMA}",
    name=f"Trino Database ({PRESTO_SCHEMA})",
    description="Trino SQL database connection"
)
def trino_resource():
    return {
        "host": PRESTO_HOST,
        "port": PRESTO_PORT,
        "schema": PRESTO_SCHEMA
    }

# Define a query command
@app.tool(
    name="execute-query",
    description="Execute SQL Query",
    annotations=types.ToolAnnotations(
        title="Execute SQL Query",
        readOnlyHint=True
    )
)
async def execute_query(
    params: Dict[str, Any]
) -> Dict[str, Any]:
    """Execute SQL query and return results"""
    try:
        query = params.get("query")
        limit = params.get("limit", 2000)  # Default value is 2000
        
        if not query:
            return {
                "error": "Query is required"
            }
        
        # Execute query
        columns, data, row_count = PrestoService.execute_query(
            query=query,
            limit=limit
        )
        
        return {
            "columns": columns,
            "data": data,
            "row_count": row_count
        }
    except Exception as e:
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }

# Get table list
@app.tool(
    name="list-tables",
    description="List Database Tables",
    annotations=types.ToolAnnotations(
        title="List Database Tables",
        readOnlyHint=True
    )
)
async def list_tables(
    params: Dict[str, Any]
) -> Dict[str, Any]:
    """List tables in the database"""
    try:
        schema = params.get("schema", PRESTO_SCHEMA)
        
        # Query table list
        query = f"SHOW TABLES FROM {schema}"
        
        # Execute query
        df = PrestoService.execute_query_to_df(query=query)
        
        # Convert to list
        tables = df.values.tolist() if not df.empty else []
        
        return {
            "tables": tables,
            "schema": schema
        }
    except Exception as e:
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }

# Get table structure
@app.tool(
    name="describe-table",
    description="Describe Table Structure",
    annotations=types.ToolAnnotations(
        title="Describe Table Structure",
        readOnlyHint=True
    )
)
async def describe_table(
    params: Dict[str, Any]
) -> Dict[str, Any]:
    """Get table structure"""
    try:
        schema = params.get("schema", PRESTO_SCHEMA)
        table = params.get("table")
        
        if not table:
            return {
                "error": "Table name is required"
            }
        
        # Query table structure
        query = f"DESCRIBE {schema}.{table}"
        
        # Execute query
        df = PrestoService.execute_query_to_df(query=query)
        
        # Convert to dictionary list
        columns = []
        if not df.empty:
            columns = df.to_dict(orient="records")
        
        return {
            "columns": columns,
            "table": table,
            "schema": schema
        }
    except Exception as e:
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }

# Health check
@app.tool(
    name="health-check",
    description="Health Check",
    annotations=types.ToolAnnotations(
        title="Health Check",
        readOnlyHint=True
    )
)
async def health_check(
    params: Dict[str, Any]
) -> Dict[str, Any]:
    """Check service health status"""
    try:
        # Try to get a connection to verify Presto connection is normal
        conn = PrestoService.get_connection()
        PrestoService.release_connection(conn)
        status = "healthy"
    except Exception as e:
        status = "unhealthy"
        return {
            "status": status,
            "service": "mcp-trino-python",
            "host": PRESTO_HOST,
            "port": PRESTO_PORT,
            "schema": PRESTO_SCHEMA,
            "error": str(e)
        }
    
    return {
        "status": status,
        "service": "mcp-trino-python",
        "host": PRESTO_HOST,
        "port": PRESTO_PORT,
        "schema": PRESTO_SCHEMA
    }

# Prevent signal handler from being called multiple times
_is_shutting_down = False
is_valid = False

# Define signal handler
def signal_handler(sig, frame):
    global _is_shutting_down
    if _is_shutting_down:
        return
    
    _is_shutting_down = True
    if is_valid:
        PrestoService.close_all()
    print("\n👋 Gracefully shutting down the server...")
    
    # Simple direct exit method, avoiding event loop problems
    sys.exit(0)

# Main function
async def main():
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    global is_valid
    
    # Print current configuration
    print_config()
    
    # Check connection during initialization
    if not is_valid:
        try:
            print("Checking connection to Presto/Trino server...")
            conn = PrestoService.get_connection()
            PrestoService.release_connection(conn)
            is_valid = True
            print("✅ Server is ready to use! Presto/Trino connection verified.")
        except Exception as e:
            print("⚠️ WARNING: Failed to connect to Presto/Trino server:")
            print(f"    Error: {str(e)}")
            print("    Server will continue to run, but commands may fail.")
            print("    Please check your connection parameters.")
            time.sleep(5)
    
    # Use FastMCP's async stdio method
    await app.run_stdio_async()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Already handled by signal handler, no additional code needed
        pass
    except Exception as e:
        try:
            print(f"❌ Fatal error: {str(e)}")
            traceback.print_exc()
        except:
            # Prevent I/O errors during shutdown
            pass 
