select
 sum(l_extendedprice * l_discount) as revenue
from
 lineitem
where
 l_shipdate >= cast('1994-01-01' as date)
 and l_shipdate < cast('1995-01-01' as date)
 and l_discount between 0.05 and 0.07
 and l_quantity < 24;
