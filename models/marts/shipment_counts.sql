WITH fct_order_items AS (
  /* order items fact table */
  SELECT
    *
  FROM {{ ref('snowflake_hub', 'fct_order_items') }}
), filter_1 AS (
  SELECT
    *
  FROM fct_order_items
  WHERE
    RETURN_FLAG <> 'returned' AND RETURN_FLAG <> 'awaiting return'
), formula_1 AS (
  SELECT
    *
  FROM filter_1
), aggregate_1 AS (
  SELECT
    SHIP_DATE,
    SHIP_MODE,
    COUNT(ORDER_KEY) AS num_shipments
  FROM formula_1
  GROUP BY
    SHIP_DATE,
    SHIP_MODE
), shipment_counts_sql AS (
  SELECT
    *
  FROM aggregate_1
)
SELECT
  *
FROM shipment_counts_sql