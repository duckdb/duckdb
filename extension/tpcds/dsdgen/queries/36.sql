WITH results AS
  (SELECT sum(ss_net_profit) AS ss_net_profit,
          sum(ss_ext_sales_price) AS ss_ext_sales_price,
          (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin ,
          i_category ,
          i_class ,
          0 AS g_category,
          0 AS g_class
   FROM store_sales ,
        date_dim d1 ,
        item ,
        store
   WHERE d1.d_year = 2001
     AND d1.d_date_sk = ss_sold_date_sk
     AND i_item_sk = ss_item_sk
     AND s_store_sk = ss_store_sk
     AND s_state ='TN'
   GROUP BY i_category,
            i_class) ,
     results_rollup AS
  (SELECT gross_margin,
          i_category,
          i_class,
          0 AS t_category,
          0 AS t_class,
          0 AS lochierarchy
   FROM results
   UNION SELECT (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
                i_category,
                NULL AS i_class,
                0 AS t_category,
                1 AS t_class,
                1 AS lochierarchy
   FROM results
   GROUP BY i_category
   UNION SELECT (sum(ss_net_profit)*1.0000)/sum(ss_ext_sales_price) AS gross_margin,
                NULL AS i_category,
                NULL AS i_class,
                1 AS t_category,
                1 AS t_class,
                2 AS lochierarchy
   FROM results)
SELECT gross_margin,
       i_category,
       i_class,
       lochierarchy,
       rank() OVER ( PARTITION BY lochierarchy,
                                  CASE
                                      WHEN t_class = 0 THEN i_category
                                  END
                    ORDER BY gross_margin ASC) AS rank_within_parent
FROM results_rollup
ORDER BY lochierarchy DESC NULLS FIRST,
         CASE
             WHEN lochierarchy = 0 THEN i_category
         END NULLS FIRST,
         rank_within_parent NULLS FIRST
LIMIT 100;

