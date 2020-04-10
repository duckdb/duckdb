#include "catch.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <iostream>
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test TPC-H SF0.01 with relations", "[tpch][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	double sf = 0.01;

	tpch::dbgen(sf, db);

	auto lineitem = con.Table("lineitem");
	auto part = con.Table("part");
	auto supplier = con.Table("supplier");
	auto partsupp = con.Table("partsupp");
	auto nation = con.Table("nation");
	auto region = con.Table("region");
	auto orders = con.Table("orders");
	auto customer = con.Table("customer");
	// Q01
	result =
	    lineitem->Filter("l_shipdate <= DATE '1998-09-02'")
	        ->Aggregate(
	            {"l_returnflag", "l_linestatus", "sum(l_quantity) AS sum_qty", "sum(l_extendedprice) AS sum_base_price",
	             "sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price",
	             "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge", "avg(l_quantity) AS avg_qty",
	             "avg(l_extendedprice) AS avg_price", "avg(l_discount) AS avg_disc", "count(*) AS count_order"})
	        ->Order("l_returnflag, l_linestatus")
	        ->Execute();
	COMPARE_CSV(result, tpch::get_answer(sf, 1), true);

	// Q02
	auto partsupp_region = partsupp->Join(supplier, "s_suppkey=ps_suppkey")
	                           ->Join(nation, "s_nationkey=n_nationkey")
	                           ->Join(region, "n_regionkey=r_regionkey");
	partsupp_region->CreateView("partsupp_region");
	auto part_join = partsupp_region->Join(part, "p_partkey=ps_partkey");
	result =
	    part_join
	        ->Filter({"p_size=15", "p_type LIKE '%BRASS'", "r_name='EUROPE'",
	                  "ps_supplycost = ( SELECT min(ps_supplycost) FROM partsupp_region WHERE p_partkey = ps_partkey "
	                  "AND r_name = 'EUROPE')"})
	        ->Project({"s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"})
	        ->Order({"s_acctbal DESC", "n_name", "s_name", "p_partkey"})
	        ->Limit(100)
	        ->Execute();
	COMPARE_CSV(result, tpch::get_answer(sf, 2), true);

	// Q03
	auto cust_join = customer->Join(orders, "c_custkey=o_custkey")->Join(lineitem, "l_orderkey=o_orderkey");
	result =
	    cust_join
	        ->Filter({"c_mktsegment = 'BUILDING'", "o_orderdate < DATE '1995-03-15'", "l_shipdate > DATE '1995-03-15'"})
	        ->Aggregate(
	            {"l_orderkey", "sum(l_extendedprice * (1 - l_discount)) AS revenue", "o_orderdate", "o_shippriority"})
	        ->Order("revenue DESC, o_orderdate")
	        ->Limit(10)
	        ->Execute();
	COMPARE_CSV(result, tpch::get_answer(sf, 3), true);

	// Q06
	result = lineitem
	             ->Filter({"l_shipdate >= cast('1994-01-01' AS date)", "l_shipdate < cast('1995-01-01' AS date)",
	                       "l_discount BETWEEN 0.05 AND 0.07", "l_quantity < 24;"})
	             ->Aggregate("sum(l_extendedprice * l_discount) AS revenue")
	             ->Execute();
	COMPARE_CSV(result, tpch::get_answer(sf, 6), true);

	// Q12
	result =
	    lineitem->Join(orders, "l_orderkey=o_orderkey")
	        ->Filter({"l_shipmode IN ('MAIL', 'SHIP')", "l_commitdate < l_receiptdate", "l_shipdate < l_commitdate",
	                  "l_receiptdate >= cast('1994-01-01' AS date)", "l_receiptdate < cast('1995-01-01' AS date)"})
	        ->Aggregate({"l_shipmode",
	                     "sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) "
	                     "AS high_line_count",
	                     "sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 "
	                     "END) AS low_line_count"})
	        ->Order("l_shipmode")
	        ->Execute();
	COMPARE_CSV(result, tpch::get_answer(sf, 12), true);
}
