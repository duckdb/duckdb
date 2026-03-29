SELECT
    t2rp1 AS g0,
    t3rp1 AS g1,
    t4rp1 AS g2,
    CAST(avg(cc.creditCard_lastChargeAmount) AS int) AS p0,
    min(cc.creditCard_lastChargeTimestamp) AS p1,
    count(DISTINCT (cc.creditCard_holder)) AS p2
FROM CategoryView ca
LEFT OUTER JOIN
    ProductView p ON ca.category_name = p.product_categoryName
LEFT OUTER JOIN
    OrderItemView oi ON p.product_id = oi.orderItem_productId
LEFT OUTER JOIN
    OrderView o ON oi.orderItem_orderId = o.order_id
LEFT OUTER JOIN
    CreditCardView cc ON o.order_creditCardNumber = cc.creditCard_number
LEFT OUTER JOIN (
        SELECT
            sum(taxRecord_bracket) AS t1rp1,
            ca.category_name AS t1pk
        FROM CategoryView ca
        LEFT OUTER JOIN
            ProductView p ON ca.category_name = p.product_categoryName
        LEFT OUTER JOIN
            OrderItemView oi ON p.product_id = oi.orderItem_productId
        LEFT OUTER JOIN
            OrderView o ON oi.orderItem_orderId = o.order_id
        LEFT OUTER JOIN
            CustomerView c ON o.order_customerId = c.customer_id
        LEFT OUTER JOIN
            AddressView a ON c.customer_id = a.address_customerId
        LEFT OUTER JOIN
            TaxRecordView t ON a.address_id = t.taxRecord_addressId
        GROUP BY ca.category_name
    ) t1 ON ca.category_name = t1pk
LEFT OUTER JOIN (
        SELECT
            max(p.product_likes) AS t2rp1,
            ca.category_name AS t2pk
        FROM CategoryView ca
        LEFT OUTER JOIN
            ProductView p ON ca.category_name = p.product_categoryName
        GROUP BY ca.category_name
    ) t2 ON ca.category_name = t2pk
LEFT OUTER JOIN (
        SELECT
            sum(oi.orderItem_productGroup) AS t3rp1,
            ca.category_name AS t3pk
        FROM CategoryView ca
        LEFT OUTER JOIN
            ProductView p ON ca.category_name = p.product_categoryName
        LEFT OUTER JOIN
            OrderItemView oi ON p.product_id = oi.orderItem_productId
        WHERE oi.orderItem_weight > 15.0
        GROUP BY ca.category_name
    ) t3 ON ca.category_name = t3pk
LEFT OUTER JOIN (
        SELECT
            max(cc.creditCard_zip) AS t4rp1,
            ca.category_name AS t4pk
        FROM CategoryView ca
        LEFT OUTER JOIN
            ProductView p ON ca.category_name = p.product_categoryName
        LEFT OUTER JOIN
            OrderItemView oi ON p.product_id = oi.orderItem_productId
        LEFT OUTER JOIN
            OrderView o ON oi.orderItem_orderId = o.order_id
        LEFT OUTER JOIN
            CreditCardView cc ON o.order_creditCardNumber = cc.creditCard_number
        GROUP BY ca.category_name
    ) t4 ON ca.category_name = t4pk
WHERE t1rp1 > 6
GROUP BY t2rp1, t3rp1, t4rp1
ORDER BY g1, p2
LIMIT 500;
