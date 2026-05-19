SELECT
    t4rp1 AS g0,
    t5rp1 AS g1,
    sum(creditCard_lastChargeAmount) AS p0,
    min(t6rp1) AS p1,
    sum(t3rp2) AS p2
FROM CreditCardView cc
LEFT OUTER JOIN (
        SELECT min(order_id) AS t1rp1, creditCard_number AS t1pk
        FROM CreditCardView cc
        LEFT OUTER JOIN OrderView o ON cc.creditCard_number = o.order_creditCardNumber
        WHERE order_slaProbability > 0.125
        GROUP BY creditCard_number
    ) t1 ON cc.creditCard_number = t1pk
LEFT OUTER JOIN (
        SELECT sum(orderItem_weight) AS t2rp1, creditCard_number AS t2pk
        FROM CreditCardView cc
        LEFT OUTER JOIN OrderView o ON cc.creditCard_number = o.order_creditCardNumber
        LEFT OUTER JOIN OrderItemView oi ON o.order_id = oi.orderItem_orderId
        GROUP BY creditCard_number
    ) t2 ON cc.creditCard_number = t2pk
LEFT OUTER JOIN (
        SELECT
            min(address_zip) AS t3rp1,
            sum(taxRecord_bracketThreshold) AS t3rp2,
            creditCard_number AS t3pk
        FROM CreditCardView cc
        LEFT OUTER JOIN OrderView o ON cc.creditCard_number = o.order_creditCardNumber
        LEFT OUTER JOIN CustomerView c ON o.order_customerId = c.customer_id
        LEFT OUTER JOIN AddressView a ON c.customer_id = a.address_customerId
        LEFT OUTER JOIN TaxRecordView t ON a.address_id = t.taxRecord_addressId
        GROUP BY creditCard_number
    ) t3 ON cc.creditCard_number = t3pk
LEFT OUTER JOIN (
        SELECT sum(product_price) AS t4rp1, creditCard_number AS t4pk
        FROM CreditCardView cc
        LEFT OUTER JOIN OrderView o ON cc.creditCard_number = o.order_creditCardNumber
        LEFT OUTER JOIN OrderItemView oi ON o.order_id = oi.orderItem_orderId
        LEFT OUTER JOIN ProductView p ON oi.orderItem_productId = p.product_id
        WHERE orderItem_weight < 25.0
        GROUP BY creditCard_number
    ) t4 ON cc.creditCard_number = t4pk
LEFT OUTER JOIN (
        SELECT sum(category_regulationProbability) AS t5rp1, creditCard_number AS t5pk
        FROM CreditCardView cc
        LEFT OUTER JOIN OrderView o ON cc.creditCard_number = o.order_creditCardNumber
        LEFT OUTER JOIN OrderItemView oi ON o.order_id = oi.orderItem_orderId
        LEFT OUTER JOIN ProductView p ON oi.orderItem_productId = p.product_id
        LEFT OUTER JOIN CategoryView ca ON p.product_categoryName = ca.category_name
        GROUP BY creditCard_number
    ) t5 ON cc.creditCard_number = t5pk
LEFT OUTER JOIN (
        SELECT min(product_inventoryLastOrderedOn) AS t6rp1, creditCard_number AS t6pk
        FROM CreditCardView cc
        LEFT OUTER JOIN OrderView o ON cc.creditCard_number = o.order_creditCardNumber
        LEFT OUTER JOIN OrderItemView oi ON o.order_id = oi.orderItem_orderId
        LEFT OUTER JOIN ProductView p ON oi.orderItem_productId = p.product_id
        LEFT OUTER JOIN CategoryView ca ON p.product_categoryName = ca.category_name
        WHERE product_price < 200.0
        GROUP BY creditCard_number
    ) t6 ON cc.creditCard_number = t6pk
WHERE t1rp1 > 10000 OR t2rp1 > 15 OR t3rp1 > 20200
GROUP BY t4rp1, t5rp1
ORDER BY p0, p1, p2
LIMIT 500;
