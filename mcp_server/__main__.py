"""Run the MCP server with Streamable HTTP (for ChatGPT / OpenAI)."""

from __future__ import annotations

from mcp_server.server import mcp




if __name__ == "__main__":
    # Streamable HTTP is supported by OpenAI/ChatGPT. Bind to 0.0.0.0 for remote access (e.g. ngrok).

    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=8000,
        path="/mcp",
    )
