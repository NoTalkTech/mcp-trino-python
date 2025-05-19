#!/usr/bin/env python3

import asyncio
import json
import pandas as pd
import traceback
from typing import Any, Dict, List, Optional

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from app.services.presto_service import PrestoService
from app.config.settings import PRESTO_HOST, PRESTO_PORT, PRESTO_SCHEMA, print_config

# 创建MCP服务器
app = Server("mcp-trino-python")

# 资源列表
@app.list_resources()
async def list_resources() -> list[types.Resource]:
    """返回可用资源列表"""
    return [
        types.Resource(
            uri=f"trino://{PRESTO_HOST}:{PRESTO_PORT}/{PRESTO_SCHEMA}",
            name=f"Trino Database ({PRESTO_SCHEMA})"
        )
    ]

# 定义工具列表
@app.list_tools()
async def list_tools() -> list[types.Tool]:
    """列出可用的工具"""
    return [
        types.Tool(
            name="execute-query",
            title="Execute SQL Query",
            description="执行SQL查询并返回结果",
            resource_uri_patterns=["trino://*"]
        ),
        types.Tool(
            name="list-tables",
            title="List Database Tables",
            description="列出数据库中的表",
            resource_uri_patterns=["trino://*"]
        ),
        types.Tool(
            name="describe-table",
            title="Describe Table Structure",
            description="获取表结构",
            resource_uri_patterns=["trino://*"]
        ),
        types.Tool(
            name="health-check",
            title="Health Check",
            description="检查服务健康状态",
            resource_uri_patterns=["trino://*"]
        )
    ]

# 定义工具的实现
@app.call_tool()
async def call_tool(tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """处理工具调用请求"""
    if tool_name == "execute-query":
        return await execute_query(params)
    elif tool_name == "list-tables":
        return await list_tables(params)
    elif tool_name == "describe-table":
        return await describe_table(params)
    elif tool_name == "health-check":
        return await health_check(params)
    else:
        return {
            "error": f"Unknown tool: {tool_name}"
        }

# 实现各个工具的功能
async def execute_query(params: Dict[str, Any]) -> Dict[str, Any]:
    """执行SQL查询并返回结果"""
    try:
        query = params.get("query")
        limit = params.get("limit")
        
        if not query:
            return {
                "error": "Query is required"
            }
        
        # 执行查询
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

async def list_tables(params: Dict[str, Any]) -> Dict[str, Any]:
    """列出数据库中的表"""
    try:
        schema = params.get("schema", PRESTO_SCHEMA)
        
        # 查询表列表
        query = f"SHOW TABLES FROM {schema}"
        
        # 执行查询
        df = PrestoService.execute_query_to_df(query=query)
        
        # 转换成列表
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

async def describe_table(params: Dict[str, Any]) -> Dict[str, Any]:
    """获取表结构"""
    try:
        schema = params.get("schema", PRESTO_SCHEMA)
        table = params.get("table")
        
        if not table:
            return {
                "error": "Table name is required"
            }
        
        # 查询表结构
        query = f"DESCRIBE {schema}.{table}"
        
        # 执行查询
        df = PrestoService.execute_query_to_df(query=query)
        
        # 转换成字典列表
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

async def health_check(params: Dict[str, Any]) -> Dict[str, Any]:
    """检查服务健康状态"""
    return {
        "status": "healthy",
        "service": "mcp-trino-python",
        "host": PRESTO_HOST,
        "port": PRESTO_PORT,
        "schema": PRESTO_SCHEMA
    }

# 主函数
async def main():
    # 打印当前配置
    print_config()
    
    async with stdio_server() as streams:
        await app.run(
            streams[0],
            streams[1],
            app.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main()) 