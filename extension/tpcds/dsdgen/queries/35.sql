SELECT ca_state,
       cd_gender,
       cd_marital_status,
       cd_dep_count,
       count(*) cnt1,
       min(cd_dep_count) min1,
       max(cd_dep_count) max1,
       avg(cd_dep_count) avg1,
       cd_dep_employed_count,
       count(*) cnt2,
       min(cd_dep_employed_count) min2,
       max(cd_dep_employed_count) max2,
       avg(cd_dep_employed_count) avg2,
       cd_dep_college_count,
       count(*) cnt3,
       min(cd_dep_college_count),
       max(cd_dep_college_count),
       avg(cd_dep_college_count)
FROM customer c,
     customer_address ca,
     customer_demographics
WHERE c.c_current_addr_sk = ca.ca_address_sk
  AND cd_demo_sk = c.c_current_cdemo_sk
  AND EXISTS
    (SELECT *
     FROM store_sales,
          date_dim
     WHERE c.c_customer_sk = ss_customer_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_year = 2002
       AND d_qoy < 4)
  AND (EXISTS
         (SELECT *
          FROM web_sales,
               date_dim
          WHERE c.c_customer_sk = ws_bill_customer_sk
            AND ws_sold_date_sk = d_date_sk
            AND d_year = 2002
            AND d_qoy < 4)
       OR EXISTS
         (SELECT *
          FROM catalog_sales,
               date_dim
          WHERE c.c_customer_sk = cs_ship_customer_sk
            AND cs_sold_date_sk = d_date_sk
            AND d_year = 2002
            AND d_qoy < 4))
GROUP BY ca_state,
         cd_gender,
         cd_marital_status,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
ORDER BY ca_state NULLS FIRST,
         cd_gender NULLS FIRST,
         cd_marital_status NULLS FIRST,
         cd_dep_count NULLS FIRST,
         cd_dep_employed_count NULLS FIRST,
         cd_dep_college_count NULLS FIRST
LIMIT 100;

