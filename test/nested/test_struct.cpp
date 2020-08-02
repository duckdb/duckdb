#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"

#include "dbgen.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test packing and unpacking lineitem into structs", "[nested][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	// con.EnableQueryVerification(); // FIXME something odd happening here

	auto sf = 0.01;

	tpch::dbgen(sf, db, DEFAULT_SCHEMA, "_org");
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE VIEW lineitem AS SELECT STRUCT_EXTRACT(struct, 'orderkey') l_orderkey, STRUCT_EXTRACT(struct, "
	    "'partkey') l_partkey, STRUCT_EXTRACT(struct, 'suppkey') l_suppkey, STRUCT_EXTRACT(struct, 'linenumber') "
	    "l_linenumber, STRUCT_EXTRACT(struct, 'quantity') l_quantity, STRUCT_EXTRACT(struct, 'extendedprice') "
	    "l_extendedprice, STRUCT_EXTRACT(struct, 'discount') l_discount, STRUCT_EXTRACT(struct, 'tax') l_tax, "
	    "STRUCT_EXTRACT(struct, 'returnflag') l_returnflag, STRUCT_EXTRACT(struct, 'linestatus') l_linestatus, "
	    "STRUCT_EXTRACT(struct, 'shipdate') l_shipdate, STRUCT_EXTRACT(struct, 'commitdate') l_commitdate, "
	    "STRUCT_EXTRACT(struct, 'receiptdate') l_receiptdate, STRUCT_EXTRACT(struct, 'shipinstruct') l_shipinstruct, "
	    "STRUCT_EXTRACT(struct, 'shipmode') l_shipmode, STRUCT_EXTRACT(struct, 'comment') l_comment FROM (SELECT "
	    "STRUCT_PACK(quantity := l_quantity , extendedprice := l_extendedprice , discount := l_discount , tax := l_tax "
	    ", returnflag := l_returnflag , linestatus := l_linestatus , shipdate := l_shipdate , commitdate := "
	    "l_commitdate , receiptdate := l_receiptdate , shipinstruct := l_shipinstruct , shipmode := l_shipmode , "
	    "comment := l_comment , orderkey := l_orderkey , partkey := l_partkey , suppkey := l_suppkey , linenumber := "
	    "l_linenumber) struct FROM lineitem_org) structs"));

	result = con.Query(tpch::get_query(1));
	COMPARE_CSV(result, tpch::get_answer(sf, 1), true);
}
