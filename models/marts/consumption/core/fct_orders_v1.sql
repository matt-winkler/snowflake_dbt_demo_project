WITH fct_orders AS (
  SELECT
    ORDER_KEY,
    CUSTOMER_KEY,
    ORDER_DATE,
    STATUS_CODE,
    PRIORITY_CODE,
    CLERK_NAME,
    SHIP_PRIORITY,
    ORDER_COUNT,
    RETURN_COUNT,
    GROSS_ITEM_SALES_AMOUNT,
    ITEM_DISCOUNT_AMOUNT,
    ITEM_TAX_AMOUNT,
    NET_ITEM_SALES_AMOUNT,
    REGION
  FROM {{ ref('fct_orders') }}
), fct_orders_v1 AS (
  SELECT
    *
  FROM fct_orders
)
SELECT
  *
FROM fct_orders_v1