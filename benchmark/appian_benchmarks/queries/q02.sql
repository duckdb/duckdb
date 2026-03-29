SELECT
    a.address_state AS g0,
    t1rp1 AS g1,
    t2rp1 AS g2,
    max(t5rp1) AS p0,
    avg(t8rp1 * t8rp2) AS p1,
    max(t6rp1) AS p2,
    count(c.customer_priority) AS p3,
    coalesce(avg(t7rp1), 0.0) AS p4
FROM CustomerView c
LEFT OUTER JOIN AddressView a ON c.customer_id = a.address_customerId
LEFT OUTER JOIN TaxRecordView t ON a.address_id = t.taxRecord_addressId
LEFT OUTER JOIN (
        SELECT sum(creditCard_cvv) AS t1rp1, c.customer_id AS t1pk
        FROM CustomerView c
        LEFT OUTER JOIN CreditCardView cc ON c.customer_id = cc.creditCard_customerId
        GROUP BY c.customer_id
    ) t1 ON c.customer_id = t1.t1pk
LEFT OUTER JOIN (
        SELECT min(p.product_likes) AS t2rp1, c.customer_id AS t2pk
        FROM CustomerView c
        LEFT OUTER JOIN OrderView o ON c.customer_id = o.order_customerId
        LEFT OUTER JOIN OrderItemView oi ON o.order_id = oi.orderItem_orderId
        LEFT OUTER JOIN ProductView p ON oi.orderItem_productId = p.product_id
        LEFT OUTER JOIN CategoryView ca ON p.product_categoryName = ca.category_name
        WHERE ca.category_seasonal = TRUE
        GROUP BY c.customer_id
    ) t2 ON c.customer_id = t2.t2pk
LEFT OUTER JOIN (
        SELECT max(o.order_subShipments) AS t5rp1, c.customer_id AS t5pk
        FROM CustomerView c
        LEFT OUTER JOIN OrderView o ON c.customer_id = o.order_customerId
        GROUP BY c.customer_id
    ) t5 ON c.customer_id = t5pk
LEFT OUTER JOIN (
        SELECT max(coalesce(oi.orderItem_weight, 1)) AS t6rp1, c.customer_id AS t6pk
        FROM CustomerView c
        LEFT OUTER JOIN OrderView o ON c.customer_id = o.order_customerId
        LEFT OUTER JOIN OrderItemView oi ON o.order_id = oi.orderItem_orderId
        WHERE o.order_serverId IN (1, 3, 5)
        GROUP BY c.customer_id
    ) t6 ON c.customer_id = t6pk
LEFT OUTER JOIN (
        SELECT count(ca.category_seasonal) AS t7rp1, c.customer_id AS t7pk
        FROM CustomerView c
        LEFT OUTER JOIN OrderView o ON c.customer_id = o.order_customerId
        LEFT OUTER JOIN OrderItemView oi ON o.order_id = oi.orderItem_orderId
        LEFT OUTER JOIN ProductView p ON oi.orderItem_productId = p.product_id
        LEFT OUTER JOIN CategoryView ca ON p.product_categoryName = ca.category_name
        WHERE ca.category_perishable = TRUE
        GROUP BY c.customer_id
    ) t7 ON c.customer_id = t7pk
LEFT OUTER JOIN (
        SELECT
            sum(creditCard_zip) AS t8rp1,
            sum(creditCard_lastChargeAmount) AS t8rp2,
            c.customer_id AS t8pk
        FROM CustomerView c
        LEFT OUTER JOIN OrderView o ON c.customer_id = o.order_customerId
        LEFT OUTER JOIN CreditCardView cc ON o.order_creditCardNumber = cc.creditCard_number
        GROUP BY c.customer_id
    ) t8 ON c.customer_id = t8pk
WHERE t.taxRecord_value > 149670.0
GROUP BY a.address_state, t1rp1, t2rp1
ORDER BY g0, p0, p1
LIMIT 500;
