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
    *,
    TO_CHAR(order_date, 'YYYY-MM') AS order_year_month
  FROM filter_1
), aggregate_1 AS (
  SELECT
    SHIP_MODE,
    order_year_month,
    COUNT(ORDER_KEY) AS num_shipments
  FROM formula_1
  GROUP BY
    SHIP_MODE,
    order_year_month
), shipment_counts_sql AS (
  SELECT
    *
  FROM aggregate_1
)
SELECT
  *
FROM shipment_counts_sql