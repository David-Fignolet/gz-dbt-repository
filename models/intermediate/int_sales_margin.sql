 SELECT
      products_id,
      date_date,
      orders_id,
      revenue,
      quantity,
      purchase_price,
      ROUND(s.quantity*p.purchase_price,2) AS purchase_cost,
      ROUND(s.revenue - s.quantity*p.purchase_price, 2) AS margin,
      {{ margin_percent('s.revenue', 's.quantity * CAST(p.purchase_price AS FLOAT64)') }} AS margin_percent
  FROM {{ref("stg_raw_data__sales")}} s
  LEFT JOIN {{ref("stg_raw_data__product")}} p
      USING (products_id)