SELECT
   *
   ,c.date_date
   ,(f.operational_margin - c.ads_cost) AS ads_margin
   ,f.average_basket,
    f.operational_margin,
    c.ads_cost,
    c.ads_impression,
    c.ads_clicks ,
    f.quantity,
    f.revenue,
    f.purchase_cost,
    f.operational_margin - f.shipping_fee - f.log_cost AS margin,
    f.shipping_fee,
    f.log_cost,
    f.operational_margin - f.shipping_fee - f.log_cost AS ship_cost
FROM
    {{ ref('int_campaign_days') }} c
JOIN
    {{ ref('finance_days') }} f
ON
    c.date_date = f.date_date
ORDER BY
    c.date_date DESC