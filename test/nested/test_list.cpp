// #include "catch.hpp"
// #include "duckdb/common/file_system.hpp"
// #include "dbgen.hpp"
// #include "test_helpers.hpp"

// #include "duckdb.hpp"
// #include "duckdb/parser/parsed_data/create_table_function_info.hpp"
// #include "duckdb/function/table_function.hpp"
// #include "duckdb/function/scalar_function.hpp"
// #include "duckdb/execution/operator/list.hpp"
// #include "duckdb/catalog/catalog_entry/list.hpp"
// #include "duckdb/function/function.hpp"
// #include "duckdb/planner/expression/list.hpp"
// #include "duckdb/parser/expression/function_expression.hpp"
// #include "duckdb/main/client_context.hpp"
// #include "duckdb/function/aggregate_function.hpp"
// #include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

// using namespace duckdb;
// using namespace std;

// TEST_CASE("Test packing and unpacking lineitem into lists", "[nested][.]") {
// 	DuckDB db(nullptr);
// 	Connection con(db);
// 	unique_ptr<MaterializedQueryResult> result;
// 	con.EnableQueryVerification(); // FIXME something odd happening here
// 	auto sf = 0.01;
// 	// TODO this has a small limit in it right now because of performance issues. Fix this.
// 	tpch::dbgen(sf, db, DEFAULT_SCHEMA, "_org");

// 	REQUIRE_NO_FAIL(con.Query("CREATE TABLE lineitem_small AS SELECT * FROM lineitem_org LIMIT 1050;"));

// 	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE VIEW lineitem AS SELECT * FROM lineitem_small"));
// 	// run the regular Q1 on the small lineitem set
// 	result = con.Query(tpch::get_query(1));
// 	// construct the expected values from the regular query
// 	vector<vector<Value>> expected_values;
// 	for (idx_t col_idx = 0; col_idx < result->types.size(); col_idx++) {
// 		vector<Value> column_list;
// 		for (idx_t row_idx = 0; row_idx < result->collection.count; row_idx++) {
// 			column_list.push_back(result->GetValue(col_idx, row_idx));
// 		}
// 		expected_values.push_back(column_list);
// 	}

// 	REQUIRE_NO_FAIL(con.Query(
// 	    "CREATE OR REPLACE VIEW lineitem AS SELECT l_orderkey, STRUCT_EXTRACT(struct, 'l_partkey') l_partkey, "
// 	    "STRUCT_EXTRACT(struct, 'l_suppkey') l_suppkey, STRUCT_EXTRACT(struct, 'l_linenumber') l_linenumber, "
// 	    "STRUCT_EXTRACT(struct, 'l_quantity') l_quantity, STRUCT_EXTRACT(struct, 'l_extendedprice') l_extendedprice, "
// 	    "STRUCT_EXTRACT(struct, 'l_discount') l_discount, STRUCT_EXTRACT(struct, 'l_tax') l_tax, "
// 	    "STRUCT_EXTRACT(struct, 'l_returnflag') l_returnflag, STRUCT_EXTRACT(struct, 'l_linestatus') l_linestatus, "
// 	    "STRUCT_EXTRACT(struct, 'l_shipdate') l_shipdate, STRUCT_EXTRACT(struct, 'l_commitdate') l_commitdate, "
// 	    "STRUCT_EXTRACT(struct, 'l_receiptdate') l_receiptdate, STRUCT_EXTRACT(struct, 'l_shipinstruct') "
// 	    "l_shipinstruct, STRUCT_EXTRACT(struct, 'l_shipmode') l_shipmode, STRUCT_EXTRACT(struct, 'l_comment') "
// 	    "l_comment FROM (SELECT l_orderkey, UNLIST(rest) struct FROM (SELECT l_orderkey, LIST(STRUCT_PACK(l_partkey "
// 	    ",l_suppkey ,l_linenumber ,l_quantity ,l_extendedprice ,l_discount ,l_tax ,l_returnflag ,l_linestatus "
// 	    ",l_shipdate ,l_commitdate ,l_receiptdate ,l_shipinstruct ,l_shipmode ,l_comment)) rest FROM (SELECT * FROM "
// 	    "lineitem_small ) lss GROUP BY l_orderkey) s1) s2;"));
// 	result = con.Query(tpch::get_query(1));
// 	for (idx_t col_idx = 0; col_idx < expected_values.size(); col_idx++) {
// 		REQUIRE(CHECK_COLUMN(result, col_idx, expected_values[col_idx]));
// 	}

// 	// database as-a-value
// 	REQUIRE_NO_FAIL(con.Query(
// 	    "CREATE OR REPLACE VIEW lineitem AS SELECT STRUCT_EXTRACT(ls, 'l_orderkey') l_orderkey, STRUCT_EXTRACT(ls, "
// 	    "'l_partkey') l_partkey, STRUCT_EXTRACT(ls, 'l_suppkey') l_suppkey, STRUCT_EXTRACT(ls, 'l_linenumber') "
// 	    "l_linenumber, STRUCT_EXTRACT(ls, 'l_quantity') l_quantity, STRUCT_EXTRACT(ls, 'l_extendedprice') "
// 	    "l_extendedprice, STRUCT_EXTRACT(ls, 'l_discount') l_discount, STRUCT_EXTRACT(ls, 'l_tax') l_tax, "
// 	    "STRUCT_EXTRACT(ls, 'l_returnflag') l_returnflag, STRUCT_EXTRACT(ls, 'l_linestatus') l_linestatus, "
// 	    "STRUCT_EXTRACT(ls, 'l_shipdate') l_shipdate, STRUCT_EXTRACT(ls, 'l_commitdate') l_commitdate, "
// 	    "STRUCT_EXTRACT(ls, 'l_receiptdate') l_receiptdate, STRUCT_EXTRACT(ls, 'l_shipinstruct') l_shipinstruct, "
// 	    "STRUCT_EXTRACT(ls, 'l_shipmode') l_shipmode, STRUCT_EXTRACT(ls, 'l_comment') l_comment FROM (SELECT "
// 	    "UNNEST(lineitem) ls FROM (SELECT LIST(STRUCT_PACK(l_orderkey, l_partkey ,l_suppkey ,l_linenumber ,l_quantity "
// 	    ",l_extendedprice ,l_discount ,l_tax ,l_returnflag ,l_linestatus ,l_shipdate ,l_commitdate ,l_receiptdate "
// 	    ",l_shipinstruct ,l_shipmode ,l_comment)) lineitem FROM (SELECT * FROM lineitem_small) s1) s2) s3;"));
// 	result = con.Query(tpch::get_query(1));
// 	for (idx_t col_idx = 0; col_idx < expected_values.size(); col_idx++) {
// 		REQUIRE(CHECK_COLUMN(result, col_idx, expected_values[col_idx]));
// 	}

// 	REQUIRE_NO_FAIL(con.Query(
// 	    "CREATE OR REPLACE VIEW lineitem AS SELECT UNNEST(STRUCT_EXTRACT(lineitem, 'll_orderkey')) l_orderkey, "
// 	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_partkey')) l_partkey, UNNEST(STRUCT_EXTRACT(lineitem, 'll_suppkey')) "
// 	    "l_suppkey, UNNEST(STRUCT_EXTRACT(lineitem, 'll_linenumber')) l_linenumber, UNNEST(STRUCT_EXTRACT(lineitem, "
// 	    "'ll_quantity')) l_quantity, UNNEST(STRUCT_EXTRACT(lineitem, 'll_extendedprice')) l_extendedprice, "
// 	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_discount')) l_discount, UNNEST(STRUCT_EXTRACT(lineitem, 'll_tax')) l_tax, "
// 	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_returnflag')) l_returnflag, UNNEST(STRUCT_EXTRACT(lineitem, "
// 	    "'ll_linestatus')) l_linestatus, UNNEST(STRUCT_EXTRACT(lineitem, 'll_shipdate')) l_shipdate, "
// 	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_commitdate')) l_commitdate, UNNEST(STRUCT_EXTRACT(lineitem, "
// 	    "'ll_receiptdate')) l_receiptdate, UNNEST(STRUCT_EXTRACT(lineitem, 'll_shipinstruct')) l_shipinstruct, "
// 	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_shipmode')) l_shipmode, UNNEST(STRUCT_EXTRACT(lineitem, 'll_comment')) "
// 	    "l_comment FROM (SELECT STRUCT_PACK(ll_orderkey:= LIST(l_orderkey), ll_partkey := LIST(l_partkey ), ll_suppkey "
// 	    ":= LIST(l_suppkey ), ll_linenumber := LIST(l_linenumber ), ll_quantity := LIST(l_quantity ), ll_extendedprice "
// 	    ":= LIST(l_extendedprice ), ll_discount := LIST(l_discount ), ll_tax := LIST(l_tax ), ll_returnflag := "
// 	    "LIST(l_returnflag ), ll_linestatus := LIST(l_linestatus ), ll_shipdate := LIST(l_shipdate ), ll_commitdate := "
// 	    "LIST(l_commitdate ), ll_receiptdate := LIST(l_receiptdate ), ll_shipinstruct := LIST(l_shipinstruct ), "
// 	    "ll_shipmode := LIST(l_shipmode ), ll_comment:= LIST(l_comment)) lineitem FROM (SELECT * FROM lineitem_small) "
// 	    "s1) s2;"));
// 	result = con.Query(tpch::get_query(1));
// 	for (idx_t col_idx = 0; col_idx < expected_values.size(); col_idx++) {
// 		REQUIRE(CHECK_COLUMN(result, col_idx, expected_values[col_idx]));
// 	}
// }

// // TEST_CASE("Aggregate lists", "[nested]") {
// //	DuckDB db(nullptr);
// //	Connection con(db);
// //	con.EnableQueryVerification();
// //	unique_ptr<QueryResult> result;
// //
// //	result = con.Query("SELECT SUM(a), b FROM (VALUES (42, LIST_VALUE(1, 2)), (42, LIST_VALUE(3, 4, 5)), (24,
// // LIST_VALUE(1, 2))) lv(a, b) GROUP BY b"); 	result->Print();
// //}
