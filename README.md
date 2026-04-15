# Sql Builder Ai

> By [MEOK AI Labs](https://meok.ai) — MEOK AI Labs MCP Server

SQL Builder AI MCP Server

## Installation

```bash
pip install sql-builder-ai-mcp
```

## Usage

```bash
# Run standalone
python server.py

# Or via MCP
mcp install sql-builder-ai-mcp
```

## Tools

### `build_select`
Build a SELECT SQL query from structured parameters.

**Parameters:**
- `table` (str)
- `columns` (str)
- `where` (str)
- `order_by` (str)
- `limit` (int)
- `joins` (str)

### `build_insert`
Build an INSERT SQL query from a list of row dicts.

**Parameters:**
- `table` (str)
- `rows` (str)
- `on_conflict` (str)

### `explain_query`
Analyze and explain a SQL query's structure and components.

**Parameters:**
- `sql` (str)

### `optimize_query_hints`
Suggest optimizations for a SQL query.

**Parameters:**
- `sql` (str)


## Authentication

Free tier: 15 calls/day. Upgrade at [meok.ai/pricing](https://meok.ai/pricing) for unlimited access.

## Links

- **Website**: [meok.ai](https://meok.ai)
- **GitHub**: [CSOAI-ORG/sql-builder-ai-mcp](https://github.com/CSOAI-ORG/sql-builder-ai-mcp)
- **PyPI**: [pypi.org/project/sql-builder-ai-mcp](https://pypi.org/project/sql-builder-ai-mcp/)

## License

MIT — MEOK AI Labs
