CREATE VIEW gross_margin_view AS
SELECT
    s.store_id,
    c.category_id,
    SUM(p.product_price * p.product_count) AS sales_sum
FROM
    purchase_items p
JOIN
    products c ON p.product_id = c.product_id
JOIN
    purchases s ON p.purchase_id = s.purchase_id
GROUP BY
    s.store_id,
    c.category_id;