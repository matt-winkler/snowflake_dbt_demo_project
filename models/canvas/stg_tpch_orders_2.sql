WITH orders AS (
  SELECT
    *
  FROM {{ source('tpch', 'orders') }}
), rename_1 AS (
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
), filter_1 AS (
  SELECT
    *
  FROM rename_1
  WHERE
    TOTAL_PRICE > 100
), stg_tpch_orders_2_sql AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM stg_tpch_orders_2_sql