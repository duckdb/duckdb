select
 s_name,
 s_address
from
 supplier,
 nation
where
 s_suppkey in (
 select
 ps_suppkey
 from
 partsupp
 where
 ps_partkey in (
 select
 p_partkey
 from
 part
 where
 p_name like 'forest%'
 )
 and ps_availqty > (
 select
 0.5 * sum(l_quantity)
 from
 lineitem
 where
 l_partkey = ps_partkey
 and l_suppkey = ps_suppkey
 and l_shipdate >= cast('1994-01-01' as date)
 and l_shipdate < cast ('1995-01-01' as date)
 )
 )
 and s_nationkey = n_nationkey
 and n_name = 'CANADA'
order by
 s_name;
