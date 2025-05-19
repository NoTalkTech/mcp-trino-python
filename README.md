# MCP Trino Python

MCP server for Presto/Trino SQL queries with Cursor integration, featuring native MCP protocol support.

## Features

- Connect to Presto/Trino databases
- Execute SQL queries and retrieve results
- Streaming data with pandas DataFrame support
- Run via npx without global installation
- Native MCP protocol for direct Cursor integration

## Requirements

- Python 3.8+
- uv package manager
- Node.js 12+ (for npx usage)

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd mcp-trino-python
```

2. Set up the environment:

```bash
# Install uv if not already installed
pip install uv  # on Linux: pip install uv
brew install uv # On MacOS: brew install uv

# Create a virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv sync
```
(You can also directly run `setup.sh` to set up the environment.)

## Usage

### Running with npx

You can also run the server using npx without needing to install it globally:

```bash
# Link the package locally (run once)
cd npm-package && npm link && cd ..

# Run using npx (also supports all command-line parameters)
npx -y mcp-trino-python --username user --password pass
```

### Adding to Cursor

#### Method 1: Using JSON Configuration

You can also configure the MCP server by editing Cursor's configuration JSON directly:

1. Open Cursor settings (Cmd+, on Mac or Ctrl+, on Windows/Linux)
2. Click on "Edit in JSON" at the top right
3. Add the following to your configuration:

```json
{
  "mcpServers": {
    "trino-python": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-trino-python",
        "--username",
        "your_username", // Required
        "--password",
        "your_password" // Required
      ]
    }
  }
}
```

If you've published the package to npm, you can use this configuration:

```json
{
  "mcpServers": {
    "trino-python": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-trino-python",
        "--host",
        "your-trino-host",
        "--port",
        "443",
        "--username",
        "your_username", // Required
        "--password",
        "your_password", // Required
        "--schema",
        "your_schema"
      ]
    }
  }
}
```

Available Confurations:

| Option | Description |
|--------|-------------|
| `--host` | Presto/Trino host server |
| `--port` | Server port |
| `--protocol` | Protocol (http/https) |
| `--username` | Username for authentication (Required) |
| `--password` | Password for authentication (Required) |
| `--schema` | Default schema |
| `--source` | Source identifier |
| `--source-template` | Source template with {username} placeholder |
| `--debug` | Enable debug mode |
| `--verbose` | Show verbose output including credentials |

## Available MCP Commands

The MCP server supports the following commands:

| Command | Description |
|---------|-------------|
| `execute-query` | Execute SQL queries with optional limit |
| `list-tables` | List all tables in a schema |
| `describe-table` | Show table structure including columns |
| `health-check` | Check server health and connection status |

### Example Command Usage

In Cursor IDE, you can use these commands as follows:

1. For executing queries:
   ```
   Command: execute-query
   Parameters: {"query": "SELECT * FROM your_table LIMIT 10"}
   ```

2. For listing tables:
   ```
   Command: list-tables
   Parameters: {"schema": "your_schema"}
   ```

3. For describing a table:
   ```
   Command: describe-table
   Parameters: {"table": "your_table", "schema": "your_schema"}
   ```

## Troubleshooting

If you encounter connection issues:

1. Check network connectivity to the Presto/Trino server
2. Run with `--verbose` flag to see detailed connection information
3. Ensure proper permissions for the specified schema

## License

MIT