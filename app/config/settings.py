import os
import argparse
from dotenv import load_dotenv

# 命令行参数解析
def parse_args():
    parser = argparse.ArgumentParser(description='MCP Trino Python Server')
    
    # Presto连接参数
    parser.add_argument('--host', help='Presto host')
    parser.add_argument('--port', help='Presto port')
    parser.add_argument('--protocol', help='Presto protocol (http/https)')
    parser.add_argument('--username', help='Presto username')
    parser.add_argument('--password', help='Presto password')
    parser.add_argument('--schema', help='Presto schema')
    parser.add_argument('--source', help='Presto source')
    parser.add_argument('--source-template', help='Presto source template, use {username} for username replacement')
    
    # 详细输出设置
    parser.add_argument('--verbose', action='store_true', help='Show verbose output')
    
    return parser.parse_args()

# 加载环境变量和命令行参数
load_dotenv()
args = parse_args()

# Presto connection settings
PRESTO_HOST = args.host or os.getenv("PRESTO_HOST", "localhost")
PRESTO_PORT = args.port or os.getenv("PRESTO_PORT", "8080")
PRESTO_PROTOCOL = args.protocol or os.getenv("PRESTO_PROTOCOL", "https")
PRESTO_USERNAME = args.username or os.getenv("PRESTO_USERNAME", None)
PRESTO_PASSWORD = args.password or os.getenv("PRESTO_PASSWORD", None)
PRESTO_SCHEMA = args.schema or os.getenv("PRESTO_SCHEMA", "default")

# 处理源URL
_source_template = args.source_template or os.getenv("PRESTO_SOURCE_TEMPLATE", "")
_source = args.source or os.getenv("PRESTO_SOURCE", "")

if _source_template and PRESTO_USERNAME:
    # 使用模板和用户名生成源URL
    PRESTO_SOURCE = _source_template.format(username=PRESTO_USERNAME)
else:
    PRESTO_SOURCE = _source

# 详细输出设置
VERBOSE = args.verbose or os.getenv("VERBOSE", "False").lower() == "true"

# 显示当前配置
def print_config():
    print("=== MCP Trino Python Server Configuration ===")
    print(f"Host: {PRESTO_HOST}")
    print(f"Port: {PRESTO_PORT}")
    print(f"Protocol: {PRESTO_PROTOCOL}")
    print(f"Username: {PRESTO_USERNAME}")
    print(f"Schema: {PRESTO_SCHEMA}")
    if VERBOSE:
        print(f"Source: {PRESTO_SOURCE}")
        print(f"Password: {'*' * (len(PRESTO_PASSWORD) if PRESTO_PASSWORD else 0)}")
    print("==========================================") 