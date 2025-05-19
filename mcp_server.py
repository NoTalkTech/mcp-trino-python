#!/usr/bin/env python3

import asyncio
import json
import pandas as pd
import traceback
from typing import Any, Dict, List, Optional

import mcp.types as types
from mcp.server import FastMCP

from app.services.presto_service import PrestoService
from app.config.settings import PRESTO_HOST, PRESTO_PORT, PRESTO_SCHEMA, print_config

# 创建MCP服务器
app = FastMCP("mcp-trino-python")

# 注册Trino资源
@app.resource(
    uri=f"trino://{PRESTO_HOST}:{PRESTO_PORT}/{PRESTO_SCHEMA}",
    name=f"Trino Database ({PRESTO_SCHEMA})",
    description="Trino SQL 数据库连接"
)
def trino_resource():
    return {
        "host": PRESTO_HOST,
        "port": PRESTO_PORT,
        "schema": PRESTO_SCHEMA
    }

# 定义一个查询命令
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
    """执行SQL查询并返回结果"""
    try:
        query = params.get("query")
        limit = params.get("limit", 2000)  # 设置默认值为 2000
        
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

# 获取表列表
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

# 获取表结构
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

# 健康检查
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
    
    # 使用 FastMCP 的异步 stdio 方法
    await app.run_stdio_async()

if __name__ == "__main__":
    asyncio.run(main()) 