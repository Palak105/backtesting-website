-- ==============================
-- Base Scan Template (DuckDB)
-- ==============================
-- This query:
-- 1. Filters by timeframe
-- 2. Computes required window columns
-- 3. Applies rule conditions (injected)
-- 4. Supports pagination
--
-- NOTE:
-- - {{WINDOW_COLUMNS}} is replaced dynamically
-- - {{ENTRY_CONDITION}} is injected safely
-- ==============================

WITH data AS (
    SELECT
        *,
        {{WINDOW_COLUMNS}}
    FROM market_data
    WHERE timeframe = ?
      {{DATE_FILTERS}}
      {{MARKET_CAP_FILTER}}
),

filtered AS (
    SELECT
        DISTINCT
        Symbol,
        MarketCapCategory,
        Industry,
        Date
    FROM data
    WHERE 1 = 1
      {{ENTRY_CONDITION}}
)

SELECT *
FROM filtered
ORDER BY Date DESC
LIMIT ? OFFSET ?;
