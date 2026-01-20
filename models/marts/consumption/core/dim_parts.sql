WITH stg_tpch_parts AS (
  SELECT
    PART_KEY,
    MANUFACTURER,
    NAME,
    BRAND,
    TYPE,
    SIZE,
    CONTAINER,
    RETAIL_PRICE
  FROM {{ ref('stg_tpch_parts') }}
), order_4c76 AS (
  SELECT
    *
  FROM stg_tpch_parts
  ORDER BY
    PART_KEY ASC
), dim_parts AS (
  SELECT
    *
  FROM order_4c76
)
SELECT
  *
FROM dim_parts