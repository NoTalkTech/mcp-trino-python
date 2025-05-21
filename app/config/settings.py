import os
import argparse
from dotenv import load_dotenv

# 加载环境变量（仅供参考，不再直接使用）
load_dotenv()

# 命令行参数解析
def parse_args():
    parser = argparse.ArgumentParser(description='MCP Trino Python Server')
    
    # Presto连接参数
    parser.add_argument('--host', default="localhost", help='Presto host (default: localhost)')
    parser.add_argument('--port', default="8080", help='Presto port (default: 8080)')
    parser.add_argument('--protocol', default="https", help='Presto protocol (http/https) (default: https)')
    parser.add_argument('--username', required=True, help='Presto username (Required)')
    parser.add_argument('--password', required=True, help='Presto password (Required)')
    parser.add_argument('--catalog', default='hive', help='Trino catalog name (default: hive)')
    parser.add_argument('--schema', default="default", help='Presto schema (default: default)')
    parser.add_argument('--source', default="mcp-trino-python", help='Presto source identifier (default: mcp-trino-python). Use {username} to include username.')
    parser.add_argument('--resource-group', default=None, help='Trino resource group or queue name')
    
    # 超时设置
    parser.add_argument('--connect-timeout', type=int, default=10, help='Connection timeout in seconds (default: 10)')
    parser.add_argument('--query-timeout', type=int, default=300, help='Query timeout in seconds (default: 300)')
    
    # 详细输出设置
    parser.add_argument('--verbose', action='store_true', help='Show verbose output including credentials')
    
    return parser.parse_args()

# 解析命令行参数
args = parse_args()

# Presto connection settings
PRESTO_HOST = args.host
PRESTO_PORT = args.port
PRESTO_PROTOCOL = args.protocol
PRESTO_USERNAME = args.username
PRESTO_PASSWORD = args.password
PRESTO_SCHEMA = args.schema
PRESTO_RESOURCE_GROUP = args.resource_group
PRESTO_CATALOG = args.catalog

# 处理源URL (允许在source中使用{username}变量)
PRESTO_SOURCE = args.source.format(username=PRESTO_USERNAME) if '{username}' in args.source else args.source

# 超时设置
CONNECT_TIMEOUT = args.connect_timeout
QUERY_TIMEOUT = args.query_timeout

# 详细输出设置
VERBOSE = args.verbose

# 显示当前配置
def print_config():
    print("=== MCP Trino Python Server Configuration ===")
    print(f"Host: {PRESTO_HOST}")
    print(f"Port: {PRESTO_PORT}")
    print(f"Protocol: {PRESTO_PROTOCOL}")
    print(f"Username: {PRESTO_USERNAME}")
    print(f"Catalog: {PRESTO_CATALOG}")
    print(f"Schema: {PRESTO_SCHEMA}")
    if VERBOSE:
        print(f"Connect Timeout: {CONNECT_TIMEOUT}s")
        print(f"Query Timeout: {QUERY_TIMEOUT}s")
        print(f"Source: {PRESTO_SOURCE}")
        print(f"Resource Group: {PRESTO_RESOURCE_GROUP}")
        print(f"Password: {'*' * (len(PRESTO_PASSWORD) if PRESTO_PASSWORD else 0)}")
    print("==========================================") 