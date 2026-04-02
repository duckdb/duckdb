SELECT address_state AS g0, sum(orderItem_quantity) AS p0
FROM CustomerView c
LEFT OUTER JOIN AddressView a ON c.customer_id = a.address_customerId
LEFT OUTER JOIN OrderView o ON c.customer_id = o.order_customerId
LEFT OUTER JOIN OrderItemView oi ON o.order_id = oi.orderItem_orderId
GROUP BY address_state
ORDER BY address_state
LIMIT 500;
