"""
SQL Builder AI MCP Server
SQL query building and analysis tools powered by MEOK AI Labs.
"""

import re
import time
from collections import defaultdict
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("sql-builder-ai-mcp")

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

SQL_KEYWORDS = {"SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE", "TABLE", "INDEX",
                "JOIN", "ORDER", "GROUP", "HAVING", "LIMIT", "OFFSET", "UNION", "CREATE", "DROP"}


@mcp.tool()
def build_select(
    table: str, columns: list[str] = None, where: dict = None,
    order_by: str = "", limit: int = 0, joins: list[dict] = None,
    group_by: list[str] = None
) -> dict:
    """Build a SELECT SQL query from structured parameters.

    Args:
        table: Main table name
        columns: List of column names (default: *)
        where: Dict of column:value conditions (AND-joined)
        order_by: Column name for ORDER BY (prefix with - for DESC)
        limit: LIMIT clause value
        joins: List of dicts: {table, on, type} e.g. {table:"orders", on:"users.id=orders.user_id", type:"LEFT"}
        group_by: List of columns for GROUP BY
    """
    _check_rate_limit("build_select")
    cols = ", ".join(columns) if columns else "*"
    parts = [f"SELECT {cols}", f"FROM {table}"]
    params = []
    if joins:
        for j in joins:
            jtype = j.get("type", "INNER").upper()
            parts.append(f"{jtype} JOIN {j['table']} ON {j['on']}")
    if where:
        conditions = []
        for col, val in where.items():
            if val is None:
                conditions.append(f"{col} IS NULL")
            elif isinstance(val, list):
                placeholders = ", ".join(["?" for _ in val])
                conditions.append(f"{col} IN ({placeholders})")
                params.extend(val)
            else:
                conditions.append(f"{col} = ?")
                params.append(val)
        parts.append("WHERE " + " AND ".join(conditions))
    if group_by:
        parts.append("GROUP BY " + ", ".join(group_by))
    if order_by:
        direction = "DESC" if order_by.startswith("-") else "ASC"
        col = order_by.lstrip("-")
        parts.append(f"ORDER BY {col} {direction}")
    if limit:
        parts.append(f"LIMIT {limit}")
    sql = "\n".join(parts) + ";"
    return {"sql": sql, "params": params, "param_count": len(params)}


@mcp.tool()
def build_insert(table: str, records: list[dict], on_conflict: str = "") -> dict:
    """Build INSERT SQL statements from data records.

    Args:
        table: Target table name
        records: List of dicts representing rows to insert
        on_conflict: Conflict resolution: '', 'ignore', 'update'
    """
    _check_rate_limit("build_insert")
    if not records:
        return {"error": "No records provided"}
    columns = list(records[0].keys())
    cols_str = ", ".join(columns)
    placeholders = ", ".join(["?" for _ in columns])
    all_params = []
    for r in records:
        all_params.append([r.get(c) for c in columns])
    sql = f"INSERT INTO {table} ({cols_str})\nVALUES ({placeholders})"
    if on_conflict == "ignore":
        sql = sql.replace("INSERT", "INSERT OR IGNORE")
    elif on_conflict == "update":
        updates = ", ".join([f"{c} = excluded.{c}" for c in columns])
        sql += f"\nON CONFLICT DO UPDATE SET {updates}"
    sql += ";"
    return {"sql": sql, "columns": columns, "record_count": len(records),
            "params_per_row": len(columns), "sample_params": all_params[0] if all_params else [],
            "all_params": all_params[:10]}


@mcp.tool()
def explain_query(sql: str) -> dict:
    """Explain what a SQL query does in plain English.

    Args:
        sql: SQL query string to explain
    """
    _check_rate_limit("explain_query")
    sql_upper = sql.upper().strip()
    explanation = []
    # Detect query type
    if sql_upper.startswith("SELECT"):
        tables = re.findall(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        joins = re.findall(r'(?:LEFT|RIGHT|INNER|OUTER|CROSS)?\s*JOIN\s+(\w+)', sql, re.IGNORECASE)
        where = re.findall(r'WHERE\s+(.+?)(?:ORDER|GROUP|LIMIT|HAVING|$)', sql, re.IGNORECASE | re.DOTALL)
        cols_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        cols = cols_match.group(1).strip() if cols_match else "*"
        explanation.append(f"Retrieves {cols} from table(s): {', '.join(tables)}")
        if joins:
            explanation.append(f"Joins with table(s): {', '.join(joins)}")
        if where:
            explanation.append(f"Filters by: {where[0].strip()}")
        if re.search(r'GROUP\s+BY', sql, re.IGNORECASE):
            explanation.append("Groups results by specified columns")
        if re.search(r'ORDER\s+BY', sql, re.IGNORECASE):
            explanation.append("Orders results by specified columns")
        if re.search(r'LIMIT', sql, re.IGNORECASE):
            limit = re.search(r'LIMIT\s+(\d+)', sql, re.IGNORECASE)
            explanation.append(f"Limits to {limit.group(1)} rows" if limit else "Limits results")
    elif sql_upper.startswith("INSERT"):
        table = re.search(r'INTO\s+(\w+)', sql, re.IGNORECASE)
        explanation.append(f"Inserts data into {table.group(1) if table else 'table'}")
    elif sql_upper.startswith("UPDATE"):
        table = re.search(r'UPDATE\s+(\w+)', sql, re.IGNORECASE)
        explanation.append(f"Updates records in {table.group(1) if table else 'table'}")
    elif sql_upper.startswith("DELETE"):
        table = re.search(r'FROM\s+(\w+)', sql, re.IGNORECASE)
        explanation.append(f"Deletes records from {table.group(1) if table else 'table'}")
    else:
        explanation.append("DDL or other SQL statement")
    return {"query": sql, "explanation": " | ".join(explanation), "query_type": sql_upper.split()[0] if sql_upper else "UNKNOWN"}


@mcp.tool()
def optimize_query_hints(sql: str) -> dict:
    """Suggest optimization hints for a SQL query.

    Args:
        sql: SQL query to analyze
    """
    _check_rate_limit("optimize_query_hints")
    hints = []
    sql_upper = sql.upper()
    if "SELECT *" in sql_upper:
        hints.append({"severity": "warning", "hint": "Avoid SELECT * - specify only needed columns"})
    if "WHERE" not in sql_upper and ("UPDATE" in sql_upper or "DELETE" in sql_upper):
        hints.append({"severity": "critical", "hint": "UPDATE/DELETE without WHERE affects all rows!"})
    if sql_upper.count("JOIN") > 3:
        hints.append({"severity": "warning", "hint": "Many JOINs detected. Consider denormalizing or using CTEs."})
    if re.search(r'WHERE.*LIKE\s+[\'"]%', sql, re.IGNORECASE):
        hints.append({"severity": "warning", "hint": "Leading wildcard LIKE prevents index usage"})
    if re.search(r'WHERE.*(?:UPPER|LOWER|TRIM)\s*\(', sql, re.IGNORECASE):
        hints.append({"severity": "info", "hint": "Functions in WHERE clause may prevent index usage"})
    if "DISTINCT" in sql_upper:
        hints.append({"severity": "info", "hint": "DISTINCT can be expensive - ensure it is necessary"})
    if "ORDER BY" in sql_upper and "LIMIT" not in sql_upper:
        hints.append({"severity": "info", "hint": "ORDER BY without LIMIT sorts all results"})
    if not re.search(r'WHERE', sql, re.IGNORECASE) and "SELECT" in sql_upper:
        hints.append({"severity": "info", "hint": "No WHERE clause - full table scan"})
    where_match = re.findall(r'WHERE\s+.+?(?:AND|OR)', sql, re.IGNORECASE)
    if where_match:
        cols = re.findall(r'(\w+)\s*[=<>!]', sql)
        if cols:
            hints.append({"severity": "info", "hint": f"Consider indexes on: {', '.join(set(cols[:5]))}"})
    return {"query": sql, "hints": hints, "hint_count": len(hints),
            "score": max(0, 100 - sum(30 if h["severity"] == "critical" else 10 if h["severity"] == "warning" else 3 for h in hints))}


if __name__ == "__main__":
    mcp.run()
