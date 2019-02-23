INSERT INTO myapp.user_summary
SELECT l.user_id,
       SUM(l.quantity) AS total_quantity,
       SUM(o.total_price) AS total_price,
       SUM(o.total_discounts) AS total_discounts,
       SUM(o.total_line_items_price) AS total_line_items_price,
       SUM(o.total_price_usd) AS total_price_usd
FROM myapp.line_items l,
     myapp.orders o
WHERE l.user_id = o.user_id
AND   l.order_id = o.id
GROUP BY l.user_id
ORDER BY l.user_id;
