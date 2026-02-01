from fastapi import APIRouter, HTTPException
import duckdb
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

DUCKDB_PATH = "market_data.duckdb"   # or remove & use parquet directly

# ---------------- DB HELPERS ----------------

def get_duckdb():
    return duckdb.connect(DUCKDB_PATH, read_only=True)

# ---------------- SAFE COLUMN FETCH ----------------

def get_allowed_columns():
    try:
        con = get_duckdb()
        result = con.execute("DESCRIBE market_data").fetchall()
        con.close()
        return {row[0] for row in result}
    except Exception as e:
        logger.error(f"Error fetching allowed columns: {e}")
        return set()

ALLOWED_COLUMNS = get_allowed_columns()

# ---------------- WINDOW HELPERS ----------------

def build_column_with_lookback(col, lookback):
    if not lookback or lookback == 0:
        return f'"{col}"'
    return f'{col}_lag_{lookback}'

def build_aggregate_column(col, agg_func, start, end):
    return f'{col}_{agg_func.lower()}_{start}_{end}'

# ---------------- RECURSIVE GROUP BUILDER ----------------

def build_group(group, params, depth=0):
    clauses = []
    operator = group.get("logic", "AND").upper()

    children = group.get("children", [])
    rules = group.get("rules", [])

    def process_rule(rule, idx):
        left = rule.get("left")
        if isinstance(left, dict):
            left = left.get("key")

        if not left or left not in ALLOWED_COLUMNS:
            return None

        left_expr = build_column_with_lookback(left, rule.get("leftLookback", 0))
        op = rule.get("operator")

        right_type = rule.get("rightType")
        right_value = rule.get("rightValue")

        if right_type == "indicator" and not right_value:
            right_value = (rule.get("rightIndicator") or {}).get("key")

        if right_type == "value":
            if right_value is None or right_value == "":
                return None
            key = f"v_{depth}_{idx}"
            params[key] = right_value
            return f"{left_expr} {op} ?"

        if right_type == "indicator":
            if right_value not in ALLOWED_COLUMNS:
                return None
            right_expr = build_column_with_lookback(
                right_value, rule.get("rightLookback", 0)
            )
            return f"{left_expr} {op} {right_expr}"

        if right_type == "aggregate":
            if right_value not in ALLOWED_COLUMNS:
                return None
            agg_expr = build_aggregate_column(
                right_value,
                rule.get("aggregateFunction"),
                rule.get("aggregateLookbackStart") or 0,
                rule.get("aggregateLookbackEnd") or 0,
            )
            return f"{left_expr} {op} {agg_expr}"

        return None

    for i, rule in enumerate(rules):
        clause = process_rule(rule, i)
        if clause:
            clauses.append(clause)

    for i, child in enumerate(children):
        if child.get("type") == "rule":
            clause = process_rule(child, len(rules) + i)
            if clause:
                clauses.append(clause)
        else:
            nested = build_group(child, params, depth + 1)
            if nested:
                clauses.append(nested)

    if not clauses:
        return ""

    return "(" + f" {operator} ".join(clauses) + ")"

# ---------------- REQUIREMENT COLLECTION ----------------

def _rule_left(node):
    left = node.get("left")
    return left.get("key") if isinstance(left, dict) else left

def _rule_right_value(node):
    if node.get("rightType") == "indicator":
        return (node.get("rightIndicator") or {}).get("key")
    return node.get("rightValue")

def collect_requirements(node, lookbacks, aggregates):
    if node.get("type") == "rule":
        left = _rule_left(node)
        if left and node.get("leftLookback", 0) > 0:
            lookbacks.add((left, node["leftLookback"]))

        rv = _rule_right_value(node)
        if node.get("rightType") == "indicator" and rv and node.get("rightLookback", 0) > 0:
            lookbacks.add((rv, node["rightLookback"]))

        if node.get("rightType") == "aggregate":
            aggregates.add((
                rv,
                node.get("aggregateFunction"),
                node.get("aggregateLookbackStart") or 0,
                node.get("aggregateLookbackEnd") or 0,
            ))

    for c in node.get("children", []):
        collect_requirements(c, lookbacks, aggregates)

    for r in node.get("rules", []):
        collect_requirements(r, lookbacks, aggregates)

# ---------------- MAIN API ----------------

@router.post("/apply")
def apply_filters(payload: dict):
    timeframe = payload.get("timeframe")
    if not timeframe:
        raise HTTPException(status_code=400, detail="timeframe is required")

    entry_tree = payload.get("entry") or payload.get("filterTree")
    limit = min(int(payload.get("limit", 50)), 500)
    offset = max(int(payload.get("offset", 0)), 0)

    market_cap = payload.get("marketCapCategory")
    start_date = payload.get("startDate")
    end_date = payload.get("endDate")

    lookbacks, aggregates = set(), set()
    if entry_tree:
        collect_requirements(entry_tree, lookbacks, aggregates)

    window_cols = ["*"]

    for col, lb in lookbacks:
        window_cols.append(
            f'LAG("{col}", {lb}) OVER (PARTITION BY Symbol ORDER BY Date) AS {col}_lag_{lb}'
        )

    for col, fn, s, e in aggregates:
        window_cols.append(
            f'{fn}("{col}") OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN {s} PRECEDING AND {e} PRECEDING) AS {col}_{fn.lower()}_{s}_{e}'
        )

    query = f"""
    WITH data AS (
        SELECT {", ".join(window_cols)}
        FROM market_data
        WHERE timeframe = ?
    )
    SELECT DISTINCT
        Symbol,
        MarketCapCategory,
        Industry,
        Date
    FROM data
    WHERE 1=1
    """

    params = [timeframe]

    if market_cap and market_cap.lower() != "all":
        query += " AND MarketCapCategory = ?"
        params.append(market_cap)

    if start_date:
        query += " AND Date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND Date <= ?"
        params.append(end_date)

    if entry_tree:
        rule_params = {}
        clause = build_group(entry_tree, rule_params)
        if clause:
            query += f" AND {clause}"
            params.extend(rule_params.values())

    query += " ORDER BY Date DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    try:
        con = get_duckdb()
        rows = con.execute(query, params).fetchall()
        con.close()

        return {
            "companies": [
                {
                    "symbol": r[0],
                    "market_cap_category": r[1],
                    "industry": r[2],
                    "date": r[3],
                }
                for r in rows
            ]
        }

    except Exception as e:
        logger.error(f"DuckDB error: {e}")
        raise HTTPException(status_code=500, detail="Database error")
