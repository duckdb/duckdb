select
 100.00 * sum(case
 when p_type like 'PROMO%'
 then l_extendedprice * (1 - l_discount)
 else 0
 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
 lineitem,
 part
where
 l_partkey = p_partkey
 and l_shipdate >= date '1995-09-01'
 and l_shipdate < cast('1995-10-01' as date);
