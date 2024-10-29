WITH sales_margin AS (
    SELECT
        orders_id,
        date_date,
        margin
    FROM
        {{ ref("int_orders_margin") }}
),
shipping_data AS (
    SELECT
        orders_id,
        shipping_fee,
        log_cost,
        CAST(ship_cost AS FLOAT64) AS ship_cost
    FROM
        {{ ref("stg_raw_data__ship") }}
)

SELECT
    s.orders_id,
    s.date_date,
    (s.margin + sh.shipping_fee - sh.log_cost - sh.ship_cost) AS operational_margin
FROM
    sales_margin s
JOIN
    shipping_data sh
ON
    s.orders_id = sh.orders_id
