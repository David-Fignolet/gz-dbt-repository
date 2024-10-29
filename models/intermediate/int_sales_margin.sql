WITH sales_data AS (
    SELECT
        s.date_date,
        s.orders_id,
        s.products_id,
        s.quantity,
        s.revenue,
        p.purchase_price
    FROM
        {{ ref("stg_raw_data__sales") }} s
    JOIN
        {{ ref("stg_raw_data__product") }} p
    ON
        s.products_id = p.products_id
)

SELECT
    date_date,
    orders_id,
    products_id,
    quantity,
    revenue,
    purchase_price,
    ROUND((quantity * purchase_price),2) AS purchase_cost,
    ROUND(revenue - (quantity * purchase_price),2) AS margin
FROM
    sales_data

