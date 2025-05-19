#!/bin/bash
set -e

echo "Setting up MCP Trino Python environment..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "uv not found. Attempting installation..."
    # Determine OS type
    OS_TYPE="$(uname -s)"

    case "$OS_TYPE" in
        Darwin) # macOS
            if command -v brew &> /dev/null; then
                echo "macOS: Installing uv via Homebrew ('brew install uv')..."
                brew install uv
            else
                echo "macOS: Homebrew not found. Installing uv via pip ('pip install uv')..."
                # Assuming pip is available and configured for a suitable Python version
                pip install uv
            fi
            ;;
        Linux) # Linux
            echo "Linux: Installing uv via pip ('pip install uv')..."
            # Assuming pip is available and configured for a suitable Python version
            pip install uv
            ;;
        *) # Other operating systems
            echo "OS '$OS_TYPE': Attempting to install uv via pip ('pip install uv') as a fallback..."
            # Assuming pip is available and configured for a suitable Python version
            pip install uv
            ;;
    esac

    # Verify that 'uv' is now in PATH after installation attempt
    # 'set -e' at the script's start will cause exit if brew/pip commands fail.
    # This check is for cases where installation succeeded but PATH is not updated.
    if ! command -v uv &> /dev/null; then
        echo "Error: 'uv' command was not found after installation." >&2
        echo "Please ensure uv's installation directory (e.g., Homebrew's bin, pip's script directory like ~/.local/bin) is in your PATH." >&2
        echo "You might need to open a new terminal or source your shell profile (e.g., .bashrc, .zshrc)." >&2
        echo "For manual installation instructions, see: https://astral.sh/uv#installation" >&2
        exit 1
    fi
    echo "uv installed successfully."
fi

# Create virtual environment
echo "Creating virtual environment..."
uv venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies
echo "Installing dependencies with uv..."
uv sync

echo "Setup complete! You can now run the server with:"
echo "npx -y mcp-trino-python --help" 