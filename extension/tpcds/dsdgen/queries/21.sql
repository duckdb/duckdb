SELECT *
FROM
  (SELECT w_warehouse_name,
          i_item_id,
          sum(CASE
                  WHEN (cast(d_date AS date) < CAST ('2000-03-11' AS date)) THEN inv_quantity_on_hand
                  ELSE 0
              END) AS inv_before,
          sum(CASE
                  WHEN (cast(d_date AS date) >= CAST ('2000-03-11' AS date)) THEN inv_quantity_on_hand
                  ELSE 0
              END) AS inv_after
   FROM inventory,
        warehouse,
        item,
        date_dim
   WHERE i_current_price BETWEEN 0.99 AND 1.49
     AND i_item_sk = inv_item_sk
     AND inv_warehouse_sk = w_warehouse_sk
     AND inv_date_sk = d_date_sk
     AND d_date BETWEEN CAST ('2000-02-10' AS date) AND CAST ('2000-04-10' AS date)
   GROUP BY w_warehouse_name,
            i_item_id) x
WHERE (CASE
           WHEN inv_before > 0 THEN (inv_after*1.000) / inv_before
           ELSE NULL
       END) BETWEEN 2.000/3.000 AND 3.000/2.000
ORDER BY w_warehouse_name NULLS FIRST,
         i_item_id NULLS FIRST
LIMIT 100;

