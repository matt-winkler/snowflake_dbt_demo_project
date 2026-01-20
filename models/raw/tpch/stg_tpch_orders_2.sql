WITH orders AS (
  SELECT
    *
  FROM {{ source('tpch', 'orders') }}
), projection_ae35 AS (
  SELECT
    O_ORDERKEY AS ORDER_KEY,
    O_CUSTKEY AS CUSTOMER_KEY,
    O_ORDERSTATUS AS STATUS_CODE,
    O_TOTALPRICE AS TOTAL_PRICE,
    O_ORDERDATE AS ORDER_DATE,
    O_ORDERPRIORITY AS PRIORITY_CODE,
    O_CLERK AS CLERK_NAME,
    O_SHIPPRIORITY AS SHIP_PRIORITY,
    O_COMMENT AS COMMENT
  FROM orders
), stg_tpch_orders_2_sql AS (
  SELECT
    *
  FROM projection_ae35
)
SELECT
  *
FROM stg_tpch_orders_2_sql