WITH fct_order_items AS (
  /* order items fact table */
  SELECT
    *
  FROM {{ ref('snowflake_hub', 'fct_order_items') }}
), dim_customers AS (
  /* Customer dimensions table */
  SELECT
    CUSTOMER_KEY,
    REGION
  FROM {{ ref('snowflake_hub', 'dim_customers') }}
), exclude_returns AS (
  SELECT
    *
  FROM fct_order_items
  WHERE
    RETURN_FLAG <> 'returned' AND RETURN_FLAG <> 'awaiting return'
), "join" AS (
  SELECT
    dim_customers.REGION,
    exclude_returns.ORDER_ITEM_KEY
  FROM exclude_returns
  LEFT JOIN dim_customers
    USING (CUSTOMER_KEY)
), aggregation AS (
  SELECT
    REGION,
    COUNT(ORDER_ITEM_KEY) AS count_ORDER_ITEM_KEY
  FROM "join"
  GROUP BY
    REGION
), orders_by_region_sql AS (
  SELECT
    *
  FROM aggregation
)
SELECT
  *
FROM orders_by_region_sql