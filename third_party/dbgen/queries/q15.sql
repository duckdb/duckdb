select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	(select
		l_suppkey as supplier_no,
		sum(l_extendedprice * (1 - l_discount)) as total_revenue
	from
		lineitem
	where
		l_shipdate >= cast('1996-01-01' as date)
		and l_shipdate < cast('1996-04-01' as date)
	group by
		supplier_no) revenue0
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			(select
		l_suppkey as supplier_no,
		sum(l_extendedprice * (1 - l_discount)) as total_revenue
	from
		lineitem
	where
		l_shipdate >= cast('1996-01-01' as date)
		and l_shipdate < cast('1996-04-01' as date)
	group by
		supplier_no) revenue1
	)
order by
	s_suppkey;
