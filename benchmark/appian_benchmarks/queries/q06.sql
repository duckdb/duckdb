SELECT
    t1rp2 AS g0,
    sum(t1rp3) / sum(t1rp4) AS p0,
    sum(CASE
        WHEN t1rp5 > 1 THEN 1
        WHEN t2rp1 > 20200 THEN 2
        WHEN t1rp6 > 15 THEN 3
        WHEN t3rp1 > 150 THEN 4
        ELSE 5
    END) AS p1
FROM ProductView p
LEFT OUTER JOIN (
        SELECT
            avg(a.address_valuation) AS t1rp1,
            sum(a.address_zone) AS t1rp2,
            sum(a.address_zone) AS t1rp3,
            count(a.address_zone) AS t1rp4,
            avg(o.order_serverId) AS t1rp5,
            avg(c.customer_balance) AS t1rp6,
            p.product_id AS t1pk
        FROM ProductView p
        LEFT OUTER JOIN OrderItemView oi ON p.product_id = oi.orderItem_productId
        LEFT OUTER JOIN OrderView o ON oi.orderItem_orderId = o.order_id
        LEFT OUTER JOIN AddressView a ON o.order_customerId = a.address_customerId
        LEFT OUTER JOIN CustomerView c ON o.order_customerId = c.customer_id
        GROUP BY p.product_id
    ) t1 ON p.product_id = t1pk
LEFT OUTER JOIN (
        SELECT min(a.address_zip) AS t2rp1, p.product_id AS t2pk
        FROM ProductView p
        LEFT OUTER JOIN OrderItemView oi ON p.product_id = oi.orderItem_productId
        LEFT OUTER JOIN OrderView o ON oi.orderItem_orderId = o.order_id
        LEFT OUTER JOIN AddressView a ON o.order_customerId = a.address_customerId
        WHERE a.address_state IN ('PA', 'CA', 'VA', 'MA', 'ME', 'MD', 'CO', 'MO')
        GROUP BY p.product_id
    ) t2 ON p.product_id = t2pk
LEFT OUTER JOIN (
        SELECT ca.category_warehouseSqft AS t3rp1, p.product_id AS t3pk
        FROM ProductView p
        LEFT OUTER JOIN CategoryView ca ON p.product_categoryName = ca.category_name
        WHERE ca.category_seasonal = TRUE
    ) t3 ON p.product_id = t3pk
WHERE t1rp1 > 10000.0
GROUP BY t1rp2
ORDER BY p0
LIMIT 500;
