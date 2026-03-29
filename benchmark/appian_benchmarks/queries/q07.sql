SELECT
    t1rp1 AS g0,
    t2rp1 AS g1,
    c.customer_age AS g2,
    c.customer_balance AS g3,
    count(c.customer_name) AS p0,
    sum(c.customer_age) AS p1
FROM CustomerView c
LEFT OUTER JOIN
    AddressView a ON c.customer_id = a.address_customerId
LEFT OUTER JOIN
    TaxRecordView t ON a.address_id = t.taxRecord_addressId
LEFT OUTER JOIN (
        SELECT
            avg(oi.orderItem_weight) AS t1rp1,
            c.customer_id AS t1pk
        FROM CustomerView c
        LEFT OUTER JOIN
            OrderView o ON c.customer_id = o.order_customerId
        LEFT OUTER JOIN
            CreditCardView cc ON o.order_creditCardNumber = cc.creditCard_number
        LEFT OUTER JOIN
            OrderItemView oi ON o.order_id = oi.orderItem_orderId
        WHERE creditCard_cvv IN (113, 115, 117, 119, 121)
        GROUP BY c.customer_id
    ) t1 ON c.customer_id = t1pk
LEFT OUTER JOIN (
        SELECT
            avg((oi.orderItem_quantity * p.product_price) /(oi.orderItem_weight + oi.orderItem_sku)) AS t2rp1,
            c.customer_id AS t2pk
        FROM CustomerView c
        LEFT OUTER JOIN
            OrderView o ON c.customer_id = o.order_customerId
        LEFT OUTER JOIN
            OrderItemView oi ON o.order_id = oi.orderItem_orderId
        LEFT OUTER JOIN
            ProductView p ON oi.orderItem_productId = p.product_id
        LEFT OUTER JOIN
            CategoryView ca ON p.product_categoryName = ca.category_name
        WHERE
            ca.category_name IN ('Pet', 'Food', 'Game', 'Software')
        GROUP BY c.customer_id
    ) t2 ON c.customer_id = t2pk
WHERE t.taxRecord_bracketThreshold IN (22, 24, 27, 29)
GROUP BY t1rp1, t2rp1, c.customer_age, c.customer_balance
ORDER BY p0, p1
LIMIT 500;
