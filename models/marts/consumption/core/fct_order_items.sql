WITH order_items AS (
  SELECT
    *
  FROM {{ ref('order_items') }}
), part_suppliers AS (
  SELECT
    *
  FROM {{ ref('part_suppliers') }}
), projection_44ea AS (
  SELECT
    *
    RENAME (PART_KEY AS ORDER_ITEM_PART_KEY, SUPPLIER_KEY AS ORDER_ITEM_SUPPLIER_KEY)
  FROM order_items
), projection_5550 AS (
  SELECT
    *
    RENAME (PART_KEY AS PART_SUPPLIER_PART_KEY, SUPPLIER_KEY AS PART_SUPPLIER_SUPPLIER_KEY)
  FROM part_suppliers
), join_d055 AS (
  SELECT
    *
  FROM projection_44ea
  JOIN projection_5550
    ON projection_44ea.ORDER_ITEM_PART_KEY = projection_5550.PART_SUPPLIER_PART_KEY
    AND projection_44ea.ORDER_ITEM_SUPPLIER_KEY = projection_5550.PART_SUPPLIER_SUPPLIER_KEY
), formula_fac1 AS (
  SELECT
    *,
    {{ cents_to_dollars("part_supplier.cost") }} AS SUPPLIER_COST,
    'ccc' AS TEST,
    1 AS ORDER_ITEM_COUNT
  FROM join_d055
), projection_ed18 AS (
  SELECT
    ORDER_ITEM_KEY,
    ORDER_KEY,
    ORDER_DATE,
    CUSTOMER_KEY,
    ORDER_ITEM_PART_KEY AS PART_KEY,
    ORDER_ITEM_SUPPLIER_KEY AS SUPPLIER_KEY,
    ORDER_ITEM_STATUS_CODE,
    RETURN_FLAG,
    LINE_NUMBER,
    SHIP_DATE,
    COMMIT_DATE,
    RECEIPT_DATE,
    SHIP_MODE,
    SUPPLIER_COST,
    RETAIL_PRICE,
    TEST,
    BASE_PRICE,
    DISCOUNT_PERCENTAGE,
    DISCOUNTED_PRICE,
    TAX_RATE,
    NATION_KEY,
    ORDER_ITEM_COUNT,
    QUANTITY,
    GROSS_ITEM_SALES_AMOUNT,
    DISCOUNTED_ITEM_SALES_AMOUNT,
    ITEM_DISCOUNT_AMOUNT,
    ITEM_TAX_AMOUNT,
    NET_ITEM_SALES_AMOUNT
  FROM formula_fac1
), order_9304 AS (
  SELECT
    *
  FROM projection_ed18
  ORDER BY
    ORDER_DATE ASC
), fct_order_items AS (
  SELECT
    *
  FROM order_9304
)
SELECT
  *
FROM fct_order_items