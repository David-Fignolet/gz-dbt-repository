WITH sales_data AS (
    SELECT
        date_date,
        COUNT(orders_id) AS total_transactions,
        SUM(revenue) AS total_revenue,
        AVG(revenue) AS average_basket,
        SUM(quantity) AS total_quantity
    FROM
        {{ ref("stg_raw_data__sales") }}
    GROUP BY
        date_date
),
margin_data AS (
    SELECT
        orders_id,
        date_date,
        (operational_margin + total_shipping_fees - log_cost - ship_cost) AS operational_margin,
        purchase_cost,
        total_shipping_fees,
        log_cost
    FROM
        {{ ref("int_orders_operational") }}
)

SELECT
    s.date_date,
    s.total_transactions,
    s.total_revenue,
    s.average_basket,
    ROUND(SUM(m.operational_margin,2)) AS operational_margin,
    ROUND(SUM(m.purchase_cost,2)) AS total_purchase_cost,
    ROUND(SUM(m.shipping_fee,2)) AS total_shipping_fees,
    ROUND(SUM(m.log_cost,2)) AS total_log_costs,
    s.total_quantity
FROM
    sales_data s
JOIN
    margin_data m
ON
    s.date_date = m.date_date
GROUP BY
    s.date_date, s.total_transactions, s.total_revenue, s.average_basket, s.total_quantity
