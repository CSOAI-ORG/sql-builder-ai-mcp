"""
SQL Builder AI MCP Server
SQL query building and analysis tools powered by MEOK AI Labs.
"""


import sys, os
sys.path.insert(0, os.path.expanduser('~/clawd/meok-labs-engine/shared'))
from auth_middleware import check_access

import re
import time
from collections import defaultdict
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("sql-builder-ai", instructions="MEOK AI Labs MCP Server")

_call_counts: dict[str, list[float]] = defaultdict(list)
FREE_TIER_LIMIT = 50
WINDOW = 86400


def _check_rate_limit(tool_name: str) -> None:
    now = time.time()
    _call_counts[tool_name] = [t for t in _call_counts[tool_name] if now - t < WINDOW]
    if len(_call_counts[tool_name]) >= FREE_TIER_LIMIT:
        raise ValueError(f"Rate limit exceeded for {tool_name}. Free tier: {FREE_TIER_LIMIT}/day. Upgrade at https://meok.ai/pricing")
    _call_counts[tool_name].append(now)


def _quote_id(name: str) -> str:
    return f'"{name}"' if not name.isidentifier() or name.upper() in SQL_KEYWORDS else name


SQL_KEYWORDS = {"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "JOIN", "ON",
                "ORDER", "GROUP", "BY", "HAVING", "LIMIT", "OFFSET", "TABLE", "INDEX",
                "CREATE", "DROP", "ALTER", "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE", "AS"}


@mcp.tool()
def build_select(
    table: str, columns: list[str] | None = None, where: dict | None = None,
    order_by: str = "", limit: int = 0, joins: list[dict] | None = None
, api_key: str = "") -> dict:
    """Build a SELECT SQL query from structured parameters.

    Args:
        table: Main table name
        columns: List of column names (default: *)
        where: Dict of column:value conditions (AND-joined)
        order_by: Column to order by (prefix with - for DESC)
        limit: LIMIT clause (0 = no limit)
        joins: List of dicts with keys: table, on, type (LEFT/INNER/RIGHT)
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": "https://meok.ai/pricing"}

    _check_rate_limit("build_select")
    cols = ", ".join(columns) if columns else "*"
    sql = f"SELECT {cols}\nFROM {table}"
    params = []
    if joins:
        for j in joins:
            jtype = j.get("type", "LEFT").upper()
            sql += f"\n{jtype} JOIN {j['table']} ON {j['on']}"
    if where:
        conditions = []
        for col, val in where.items():
            if val is None:
                conditions.append(f"{col} IS NULL")
            elif isinstance(val, list):
                placeholders = ", ".join(["%s"] * len(val))
                conditions.append(f"{col} IN ({placeholders})")
                params.extend(val)
            else:
                conditions.append(f"{col} = %s")
                params.append(val)
        if conditions:
            sql += "\nWHERE " + " AND ".join(conditions)
    if order_by:
        direction = "DESC" if order_by.startswith("-") else "ASC"
        col = order_by.lstrip("-")
        sql += f"\nORDER BY {col} {direction}"
    if limit > 0:
        sql += f"\nLIMIT {limit}"
    return {"sql": sql + ";", "params": params, "type": "SELECT"}


@mcp.tool()
def build_insert(table: str, rows: list[dict], on_conflict: str = "", api_key: str = "") -> dict:
    """Build an INSERT SQL query from a list of row dicts.

    Args:
        table: Target table name
        rows: List of dicts (each dict is a row, keys are column names)
        on_conflict: Conflict resolution: '' (none), 'ignore', 'update'
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": "https://meok.ai/pricing"}

    _check_rate_limit("build_insert")
    if not rows:
        return {"error": "No rows provided"}
    columns = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    cols_str = ", ".join(columns)
    sql = f"INSERT INTO {table} ({cols_str})\nVALUES"
    all_params = []
    value_rows = []
    for row in rows:
        vals = [row.get(c) for c in columns]
        value_rows.append(f"({placeholders})")
        all_params.extend(vals)
    sql += "\n" + ",\n".join(value_rows)
    if on_conflict == "ignore":
        sql += "\nON CONFLICT DO NOTHING"
    elif on_conflict == "update":
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns)
        sql += f"\nON CONFLICT DO UPDATE SET {updates}"
    return {"sql": sql + ";", "params": all_params, "type": "INSERT", "row_count": len(rows)}


@mcp.tool()
def explain_query(sql: str, api_key: str = "") -> dict:
    """Analyze and explain a SQL query's structure and components.

    Args:
        sql: SQL query string to analyze
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": "https://meok.ai/pricing"}

    _check_rate_limit("explain_query")
    sql_upper = sql.upper().strip()
    query_type = "UNKNOWN"
    for t in ("SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER"):
        if sql_upper.startswith(t):
            query_type = t
            break
    components = {"type": query_type}
    tables = re.findall(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
    tables += re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)
    tables += re.findall(r'\bINTO\s+(\w+)', sql, re.IGNORECASE)
    tables += re.findall(r'\bUPDATE\s+(\w+)', sql, re.IGNORECASE)
    components["tables"] = list(set(tables))
    if re.search(r'\bWHERE\b', sql, re.IGNORECASE):
        where = re.search(r'\bWHERE\b(.+?)(?:\bORDER\b|\bGROUP\b|\bLIMIT\b|\bHAVING\b|;|$)', sql, re.IGNORECASE | re.DOTALL)
        components["where_clause"] = where.group(1).strip() if where else ""
    joins = re.findall(r'((?:LEFT|RIGHT|INNER|OUTER|CROSS|FULL)\s+)?JOIN\s+(\w+)\s+ON\s+([^)]+?)(?=\s+(?:LEFT|RIGHT|INNER|WHERE|ORDER|GROUP|LIMIT|$))', sql, re.IGNORECASE)
    if joins:
        components["joins"] = [{"type": j[0].strip() or "INNER", "table": j[1], "condition": j[2].strip()} for j in joins]
    has_subquery = "(" in sql and "SELECT" in sql_upper.split("(", 1)[-1] if "(" in sql else False
    components["has_subquery"] = has_subquery
    components["complexity"] = "simple" if len(tables) <= 1 and not has_subquery else "moderate" if len(tables) <= 3 else "complex"
    return components


@mcp.tool()
def optimize_query_hints(sql: str, api_key: str = "") -> dict:
    """Suggest optimizations for a SQL query.

    Args:
        sql: SQL query string to analyze for optimizations
    """
    allowed, msg, tier = check_access(api_key)
    if not allowed:
        return {"error": msg, "upgrade_url": "https://meok.ai/pricing"}

    _check_rate_limit("optimize_query_hints")
    hints = []
    sql_upper = sql.upper()
    if "SELECT *" in sql_upper:
        hints.append({"hint": "Avoid SELECT * - specify only needed columns", "severity": "warning", "category": "performance"})
    if "WHERE" not in sql_upper and "SELECT" in sql_upper:
        hints.append({"hint": "No WHERE clause - may scan entire table", "severity": "warning", "category": "performance"})
    if re.search(r'WHERE.*\bLIKE\s+["\']%', sql, re.IGNORECASE):
        hints.append({"hint": "Leading wildcard in LIKE prevents index usage", "severity": "warning", "category": "index"})
    if re.search(r'WHERE.*\bOR\b', sql, re.IGNORECASE):
        hints.append({"hint": "OR conditions may prevent index usage - consider UNION", "severity": "info", "category": "index"})
    if "DISTINCT" in sql_upper:
        hints.append({"hint": "DISTINCT can be expensive - ensure it's necessary", "severity": "info", "category": "performance"})
    if "ORDER BY" in sql_upper and "LIMIT" not in sql_upper:
        hints.append({"hint": "ORDER BY without LIMIT sorts all results", "severity": "info", "category": "performance"})
    if sql_upper.count("SELECT") > 1:
        hints.append({"hint": "Subquery detected - consider using JOINs or CTEs instead", "severity": "info", "category": "readability"})
    if re.search(r'WHERE.*(?:FUNCTION|UPPER|LOWER|CAST|CONVERT)\s*\(', sql, re.IGNORECASE):
        hints.append({"hint": "Function in WHERE clause prevents index usage", "severity": "warning", "category": "index"})
    tables = re.findall(r'\bFROM\s+(\w+)', sql, re.IGNORECASE) + re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)
    idx_suggestions = []
    for col_match in re.finditer(r'WHERE\s+(\w+)\s*=', sql, re.IGNORECASE):
        idx_suggestions.append(f"Consider index on {col_match.group(1)}")
    return {"hints": hints, "hint_count": len(hints), "index_suggestions": idx_suggestions,
            "tables_referenced": list(set(tables))}


if __name__ == "__main__":
    mcp.run()
