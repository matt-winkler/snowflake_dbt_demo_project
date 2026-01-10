WITH stg_tpch_customers AS (
  SELECT
    *
  FROM {{ ref('stg_tpch_customers') }}
), stg_tpch_nations AS (
  SELECT
    *
  FROM {{ ref('stg_tpch_nations') }}
), stg_tpch_regions AS (
  SELECT
    *
  FROM {{ ref('stg_tpch_regions') }}
), fct_order_items AS (
  SELECT
    *
  FROM {{ ref('fct_order_items') }}
), projection_1338 AS (
  SELECT
    *
    RENAME (CUSTOMER_KEY AS CUSTOMER_CUSTOMER_KEY, NAME AS CUSTOMER_NAME, NATION_KEY AS CUSTOMER_NATION_KEY, COMMENT AS CUSTOMER_COMMENT)
  FROM stg_tpch_customers
), projection_3ff9 AS (
  SELECT
    *
    RENAME (NAME AS NATION_NAME, NATION_KEY AS NATION_NATION_KEY, COMMENT AS NATION_COMMENT, REGION_KEY AS NATION_REGION_KEY)
  FROM stg_tpch_nations
), projection_953d AS (
  SELECT
    *
    RENAME (NAME AS REGION_NAME, COMMENT AS REGION_COMMENT, REGION_KEY AS REGION_REGION_KEY)
  FROM stg_tpch_regions
), aggregation_ffd6 AS (
  SELECT
    CUSTOMER_KEY,
    MIN(ORDER_DATE) AS FIRST_ORDER_DATE
  FROM fct_order_items
  GROUP BY
    CUSTOMER_KEY
), join_d8f1 AS (
  SELECT
    *
  FROM projection_1338
  JOIN projection_3ff9
    ON projection_1338.CUSTOMER_NATION_KEY = projection_3ff9.NATION_NATION_KEY
), projection_8093 AS (
  SELECT
    CUSTOMER_KEY AS FIRST_ORDER_DATE_CUSTOMER_KEY,
    FIRST_ORDER_DATE
  FROM aggregation_ffd6
), join_fec8 AS (
  SELECT
    *
  FROM join_d8f1
  JOIN projection_953d
    ON join_d8f1.NATION_REGION_KEY = projection_953d.REGION_REGION_KEY
), join_269e AS (
  SELECT
    *
  FROM join_fec8
  JOIN projection_8093
    ON join_fec8.CUSTOMER_CUSTOMER_KEY = projection_8093.FIRST_ORDER_DATE_CUSTOMER_KEY
), projection_89a8 AS (
  SELECT
    CUSTOMER_CUSTOMER_KEY AS CUSTOMER_KEY,
    CUSTOMER_NAME AS NAME,
    ADDRESS,
    NATION_NAME AS NATION,
    REGION_NAME AS REGION,
    PHONE_NUMBER,
    ACCOUNT_BALANCE,
    MARKET_SEGMENT,
    FIRST_ORDER_DATE
  FROM join_269e
), order_cc94 AS (
  SELECT
    *
  FROM projection_89a8
  ORDER BY
    CUSTOMER_KEY ASC
), dim_customers AS (
  SELECT
    *
  FROM order_cc94
)
SELECT
  *
FROM dim_customers