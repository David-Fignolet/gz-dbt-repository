-- models/mart/finance_days.sql

WITH sales_data AS (
    SELECT
        date_date,
        COUNT(orders_id) AS total_transactions,
        ROUND(SUM(revenue),2) AS total_revenue,
        ROUND(AVG(revenue),2) AS average_basket,
        ROUND(SUM(quantity),2) AS total_quantity
    FROM
        {{ ref('int_sales_margin') }}
    GROUP BY
        date_date
),
margin_data AS (
    SELECT
        date_date,
        ROUND(SUM(margin),2) AS total_margin,
        ROUND(SUM(purchase_cost),2) AS total_purchase_cost
    FROM
        {{ ref('int_orders_margin') }}
    GROUP BY
        date_date
),
operational_data AS (
    SELECT
        date_date,
        ROUND(SUM(operational_margin),2) AS total_operational_margin,
        ROUND(SUM(shipping_fee),2) AS total_shipping_fees,
        ROUND(SUM(log_cost),2) AS total_log_costs
    FROM
        {{ ref('int_orders_operational') }}
    GROUP BY
        date_date
)

SELECT
    s.date_date,
    s.total_transactions,
    s.total_revenue,
    s.average_basket,
    o.total_operational_margin AS operational_margin,
    m.total_purchase_cost,
    o.total_shipping_fees,
    o.total_log_costs,
    s.total_quantity
FROM
    sales_data s
JOIN
    margin_data m
ON
    s.date_date = m.date_date
JOIN
    operational_data o
ON
    s.date_date = o.date_date
