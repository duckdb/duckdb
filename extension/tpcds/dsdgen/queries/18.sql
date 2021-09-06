WITH results AS
  (SELECT i_item_id,
          ca_country,
          ca_state,
          ca_county,
          cast(cs_quantity AS decimal(12,2)) agg1,
          cast(cs_list_price AS decimal(12,2)) agg2,
          cast(cs_coupon_amt AS decimal(12,2)) agg3,
          cast(cs_sales_price AS decimal(12,2)) agg4,
          cast(cs_net_profit AS decimal(12,2)) agg5,
          cast(c_birth_year AS decimal(12,2)) agg6,
          cast(cd1.cd_dep_count AS decimal(12,2)) agg7
   FROM catalog_sales,
        customer_demographics cd1,
        customer_demographics cd2,
        customer,
        customer_address,
        date_dim,
        item
   WHERE cs_sold_date_sk = d_date_sk
     AND cs_item_sk = i_item_sk
     AND cs_bill_cdemo_sk = cd1.cd_demo_sk
     AND cs_bill_customer_sk = c_customer_sk
     AND cd1.cd_gender = 'F'
     AND cd1.cd_education_status = 'Unknown'
     AND c_current_cdemo_sk = cd2.cd_demo_sk
     AND c_current_addr_sk = ca_address_sk
     AND c_birth_month IN (1,
                           6,
                           8,
                           9,
                           12,
                           2)
     AND d_year = 1998
     AND ca_state IN ('MS',
                      'IN',
                      'ND',
                      'OK',
                      'NM',
                      'VA',
                      'MS') )
SELECT i_item_id,
       ca_country,
       ca_state,
       ca_county,
       agg1,
       agg2,
       agg3,
       agg4,
       agg5,
       agg6,
       agg7
FROM
  ( SELECT i_item_id,
           ca_country,
           ca_state,
           ca_county,
           avg(agg1) agg1,
           avg(agg2) agg2,
           avg(agg3) agg3,
           avg(agg4) agg4,
           avg(agg5) agg5,
           avg(agg6) agg6,
           avg(agg7) agg7
   FROM results
   GROUP BY i_item_id,
            ca_country,
            ca_state,
            ca_county
   UNION ALL SELECT i_item_id,
                    ca_country,
                    ca_state,
                    NULL AS county,
                    avg(agg1) agg1,
                    avg(agg2) agg2,
                    avg(agg3) agg3,
                    avg(agg4) agg4,
                    avg(agg5) agg5,
                    avg(agg6) agg6,
                    avg(agg7) agg7
   FROM results
   GROUP BY i_item_id,
            ca_country,
            ca_state
   UNION ALL SELECT i_item_id,
                    ca_country,
                    NULL AS ca_state,
                    NULL AS county,
                    avg(agg1) agg1,
                    avg(agg2) agg2,
                    avg(agg3) agg3,
                    avg(agg4) agg4,
                    avg(agg5) agg5,
                    avg(agg6) agg6,
                    avg(agg7) agg7
   FROM results
   GROUP BY i_item_id,
            ca_country
   UNION ALL SELECT i_item_id,
                    NULL AS ca_country,
                    NULL AS ca_state,
                    NULL AS county,
                    avg(agg1) agg1,
                    avg(agg2) agg2,
                    avg(agg3) agg3,
                    avg(agg4) agg4,
                    avg(agg5) agg5,
                    avg(agg6) agg6,
                    avg(agg7) agg7
   FROM results
   GROUP BY i_item_id
   UNION ALL SELECT NULL AS i_item_id,
                    NULL AS ca_country,
                    NULL AS ca_state,
                    NULL AS county,
                    avg(agg1) agg1,
                    avg(agg2) agg2,
                    avg(agg3) agg3,
                    avg(agg4) agg4,
                    avg(agg5) agg5,
                    avg(agg6) agg6,
                    avg(agg7) agg7
   FROM results ) foo
ORDER BY ca_country NULLS FIRST,
         ca_state NULLS FIRST,
         ca_county NULLS FIRST,
         i_item_id NULLS FIRST
LIMIT 100;

