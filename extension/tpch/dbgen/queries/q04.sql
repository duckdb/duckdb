select
 o_orderpriority,
 count(*) as order_count
from
 orders
where
 o_orderdate >= cast('1993-07-01' as date)
 and o_orderdate < cast('1993-10-01' as date)
 and exists (
 select
 *
 from
 lineitem
 where
 l_orderkey = o_orderkey
 and l_commitdate < l_receiptdate
 )
group by
 o_orderpriority
order by
 o_orderpriority;
