SELECT t.taxRecord_rate AS g0, t2rp1 AS g1, min(c.customer_balance) AS p0
FROM TaxRecordView t
LEFT OUTER JOIN AddressView a ON t.taxRecord_addressId = a.address_id
LEFT OUTER JOIN CustomerView c ON a.address_customerId = c.customer_id
LEFT OUTER JOIN (
        SELECT min(o.order_placedOn) AS t1rp1, t.taxRecord_id AS t1pk
        FROM TaxRecordView t
        LEFT OUTER JOIN AddressView a ON t.taxRecord_addressId = a.address_id
        LEFT OUTER JOIN OrderView o ON a.address_customerId = o.order_customerId
        GROUP BY t.taxRecord_id
    ) t1 ON t.taxRecord_id = t1pk
LEFT OUTER JOIN (
        SELECT sum(p.product_price * oi.orderItem_quantity) AS t2rp1, t.taxRecord_id AS t2pk
        FROM TaxRecordView t
        LEFT OUTER JOIN AddressView a ON t.taxRecord_addressId = a.address_id
        LEFT OUTER JOIN OrderView o ON a.address_customerId = o.order_customerId
        LEFT OUTER JOIN OrderItemView oi ON o.order_id = oi.orderItem_orderId
        LEFT OUTER JOIN ProductView p ON oi.orderItem_productId = p.product_id
        GROUP BY t.taxRecord_id
    ) t2 ON t.taxRecord_id = t2pk
WHERE t1rp1 > '2020-01-14 12:12:30.0'
GROUP BY t.taxRecord_rate, t2rp1
ORDER BY p0
LIMIT 500;
