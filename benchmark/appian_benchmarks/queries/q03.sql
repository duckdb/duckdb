SELECT
    c.customer_priority AS g0,
    t1rp1 AS g1,
    t.taxRecord_bracket AS g2,
    sum(oi.orderItem_weight) AS p0,
    max(ca.category_demandScore) AS p1,
    max(ca.category_auditDate) AS p2,
    CAST(avg(ca.category_valuation) AS int) AS p3,
    sum(t1rp2) AS p4,
    sum(
        CASE
            WHEN p.product_inventoryLastOrderedOn - ca.category_auditDate > 300 THEN 1
            WHEN p.product_inventoryLastOrderedOn - ca.category_auditDate > 150 THEN 10
            WHEN p.product_inventoryLastOrderedOn - ca.category_auditDate > 0 THEN 100
            ELSE 1000
        END +(c.customer_priority * a.address_zone)) AS p5
FROM OrderItemView oi
LEFT OUTER JOIN OrderView o ON oi.orderItem_orderId = o.order_id
LEFT OUTER JOIN ProductView p ON oi.orderItem_productId = p.product_id
LEFT OUTER JOIN CreditCardView cc ON o.order_creditCardNumber = cc.creditCard_number
LEFT OUTER JOIN CustomerView c ON o.order_customerId = c.customer_id
LEFT OUTER JOIN AddressView a ON c.customer_id = a.address_customerId
LEFT OUTER JOIN TaxRecordView t ON a.address_id = t.taxRecord_addressId
LEFT OUTER JOIN CategoryView ca ON p.product_categoryName = ca.category_name
LEFT OUTER JOIN (
        SELECT
            min(cc.creditCard_expirationDate) AS t1rp1,
            sum(cc.creditCard_lastChargeAmount) AS t1rp2,
            c.customer_id AS t1pk
        FROM CustomerView c
        LEFT OUTER JOIN CreditCardView cc ON c.customer_id = cc.creditCard_customerId
        GROUP BY c.customer_id
    ) t1 ON c.customer_id = t1pk
WHERE cc.creditCard_lastChargeAmount > 90.0 AND p.product_price > 34.0
GROUP BY c.customer_priority, t1rp1, t.taxRecord_bracket
ORDER BY p1, p3, g2
LIMIT 500;
