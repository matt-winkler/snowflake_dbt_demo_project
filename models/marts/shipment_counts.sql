WITH fct_order_items AS (
  /* order items fact table */
  SELECT
    *
  FROM {{ ref('snowflake_hub', 'fct_order_items') }}
), exclude_returns AS (
  SELECT
    *
  FROM fct_order_items
  WHERE
    RETURN_FLAG <> 'returned' AND RETURN_FLAG <> 'awaiting return'
), format_dates AS (
  SELECT
    *,
    TO_CHAR(order_date, 'YYYY-MM') AS order_year_month
  FROM exclude_returns
), group_by_ship_mode_and_year_month AS (
  SELECT
    SHIP_MODE,
    order_year_month,
    COUNT(ORDER_KEY) AS num_shipments
  FROM format_dates
  GROUP BY
    SHIP_MODE,
    order_year_month
), shipment_counts_sql AS (
  SELECT
    *
  FROM group_by_ship_mode_and_year_month
)
SELECT
  *
FROM shipment_counts_sql