#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar lists", "[nested]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	unique_ptr<QueryResult> result;

	con.Query("CREATE TABLE list_data (g INTEGER, e INTEGER)");
	con.Query("INSERT INTO list_data VALUES (1, 1), (1, 2), (2, 3), (2, 4), (2, 5), (3, 6), (5, NULL)");

	result = con.Query("SELECT LIST_VALUE(1, 2, 3, '4') a, LIST_VALUE('a','b','c') b, LIST_VALUE(42, NULL) c, "
	                   "LIST_VALUE(NULL, NULL, NULL) d, LIST_VALUE() e");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({1, 2, 3, 4})}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::LIST({"a", "b", "c"})}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::LIST({42, Value()})}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value::LIST({Value(), Value(), Value()})}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value::LIST({})}));

	result = con.Query(
	    "SELECT a FROM (VALUES (LIST_VALUE(1, 2, 3, 4)), (LIST_VALUE()), (LIST_VALUE(NULL)), (LIST_VALUE(42))) lv(a)");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({1, 2, 3, 4}), Value::LIST({}), Value::LIST({Value()}), Value::LIST({42})}));

	result = con.Query("SELECT * FROM (VALUES ((LIST_VALUE()), (LIST_VALUE(NULL)), LIST_VALUE(1, 2))) lv(a)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({})}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::LIST({Value()})}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::LIST({Value::INTEGER(1), Value::INTEGER(2)})}));

	result = con.Query("SELECT * FROM (VALUES (LIST_VALUE(1, 2)), (LIST_VALUE()), (LIST_VALUE(NULL))) lv(a)");
	REQUIRE(CHECK_COLUMN(
	    result, 0, {Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}), Value::LIST({}), Value::LIST({Value()})}));

	// casting null to list or empty list to something else should work
	result = con.Query("SELECT LIST_VALUE(1, 2, 3) UNION ALL SELECT LIST_VALUE(NULL) UNION ALL SELECT LIST_VALUE() "
	                   "UNION ALL SELECT NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({1, 2, 3}), Value::LIST({Value()}), Value::LIST({}), Value()}));

	result = con.Query(" SELECT NULL UNION ALL SELECT LIST_VALUE() UNION ALL SELECT LIST_VALUE(NULL) UNION ALL SELECT "
	                   "LIST_VALUE(1, 2, 3)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value::LIST({}), Value::LIST({Value()}), Value::LIST({1, 2, 3})}));

	// empty list should not abort UNNEST
	result = con.Query("SELECT UNNEST(a) ua FROM (VALUES (LIST_VALUE(1, 2, 3, 4)), (LIST_VALUE()), (LIST_VALUE(NULL)), "
	                   "(LIST_VALUE(42))) lv(a)");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, Value(), 42}));

	// TODO this should work but does not. its also kind of obscure
	//	result = con.Query("SELECT UNNEST(a) ua FROM (VALUES (LIST_VALUE()), (LIST_VALUE(1, 2, 3, 4)),
	//(LIST_VALUE(NULL)), "
	//	                   "(LIST_VALUE(42))) lv(a)");
	//	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, Value(), 42}));
	//
	// list child type mismatch
	REQUIRE_FAIL(con.Query("SELECT * FROM (VALUES (LIST_VALUE(1, 2)), (LIST_VALUE()), (LIST_VALUE('a'))) lv(a)"));

	// can't cast lists to stuff
	REQUIRE_FAIL(con.Query("SELECT CAST(LIST_VALUE(42) AS INTEGER)"));

	// can't add a number to a list
	REQUIRE_FAIL(con.Query("SELECT LIST_VALUE(42) + 4"));

	// can't add a number to a list
	REQUIRE_FAIL(con.Query("SELECT LIST_VALUE(42, 'a')"));

	// can have unnest anywhere
	result = con.Query("SELECT CAST(UNNEST(LIST_VALUE(42))+2 AS INTEGER)");
	REQUIRE(CHECK_COLUMN(result, 0, {44}));

	result = con.Query("SELECT LIST_VALUE(g, e, 42, NULL) FROM list_data WHERE g > 2");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({3, 6, 42, Value()}), Value::LIST({5, Value(), 42, Value()})}));

	result = con.Query("SELECT CASE WHEN g = 2 THEN LIST_VALUE(g, e, 42) ELSE LIST_VALUE(84, NULL) END FROM list_data "
	                   "WHERE g > 1 UNION ALL SELECT LIST_VALUE(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({2, 3, 42}), Value::LIST({2, 4, 42}), Value::LIST({2, 5, 42}),
	                      Value::LIST({84, Value()}), Value::LIST({84, Value()}), Value::LIST({Value()})}));

	// this should fail because the list child types do not match
	REQUIRE_FAIL(con.Query(
	    "SELECT CASE WHEN g = 2 THEN LIST_VALUE(g, e, 42) ELSE LIST_VALUE('eeek') END FROM list_data	WHERE g > 1"));
}

TEST_CASE("Test filter and projection of nested lists", "[nested]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	unique_ptr<QueryResult> result;

	con.Query("CREATE TABLE list_data (g INTEGER, e INTEGER)");
	con.Query("INSERT INTO list_data VALUES (1, 1), (1, 2), (2, 3), (2, 4), (2, 5), (3, 6), (5, NULL)");

	result = con.Query("SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)})}));

	result = con.Query("SELECT UNNEST(l1) FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)) t1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));

	result = con.Query("SELECT * FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)) t1, (SELECT LIST(b) l2 "
	                   "FROM (VALUES (4), (5), (6), (7)) AS t2 (b)) t2");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)})}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value::INTEGER(7)})}));

	result = con.Query("SELECT UNNEST(l1) u1, UNNEST(l2) u2 FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 "
	                   "(a)) t1, (SELECT LIST(b) l2 FROM (VALUES (4), (5), (6), (7)) AS t2 (b)) t2");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 6, 7}));

	result = con.Query("SELECT UNNEST(l1), l2 FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)) t1, 	"
	                   "(SELECT LIST(b) l2 FROM (VALUES (4), (5), (6), (7)) AS t2 (b)) t2");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value::INTEGER(7)}),
	                      Value::LIST({Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value::INTEGER(7)}),
	                      Value::LIST({Value::INTEGER(4), Value::INTEGER(5), Value::INTEGER(6), Value::INTEGER(7)})}));

	result = con.Query("SELECT l1, UNNEST(l2) FROM (SELECT LIST(a) l1 FROM (VALUES (1), (2), (3)) AS t1 (a)) t1, "
	                   "(SELECT LIST(b) l2 FROM (VALUES (4), (5), (6), (7)) AS t2 (b)) t2");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3)})}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 5, 6, 7}));

	result = con.Query("SELECT UNNEST(LIST(e)) ue, LIST(g) from list_data");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5),
	                      Value::INTEGER(6), Value()}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)})}));

	result = con.Query("SELECT g, LIST(e) from list_data GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}),
	                      Value::LIST({Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(6)}), Value::LIST({Value()})}));

	result = con.Query("SELECT g, LIST(e) l1, LIST(e) l2 from list_data GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}),
	                      Value::LIST({Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(6)}), Value::LIST({Value()})}));
	REQUIRE(CHECK_COLUMN(result, 2,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}),
	                      Value::LIST({Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5)}),
	                      Value::LIST({Value::INTEGER(6)}), Value::LIST({Value()})}));

	result = con.Query("SELECT g, LIST(e/2.0) from list_data GROUP BY g order by g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::DOUBLE(0.5), Value::DOUBLE(1)}),
	                      Value::LIST({Value::DOUBLE(1.5), Value::DOUBLE(2), Value::DOUBLE(2.5)}),
	                      Value::LIST({Value::DOUBLE(3)}), Value::LIST({Value()})}));

	result = con.Query("SELECT g, LIST(CAST(e AS VARCHAR)) from list_data GROUP BY g order by g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value("1"), Value("2")}), Value::LIST({Value("3"), Value("4"), Value("5")}),
	                      Value::LIST({Value("6")}), Value::LIST({Value()})}));

	result = con.Query("SELECT LIST(e) from list_data");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(4),
	                                   Value::INTEGER(5), Value::INTEGER(6), Value()})}));

	result = con.Query("SELECT UNNEST(LIST(e)) ue from list_data ORDER BY ue");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4, 5, 6}));

	result = con.Query("SELECT LIST(e), LIST(g) from list_data");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(4),
	                                   Value::INTEGER(5), Value::INTEGER(6), Value()})}));
	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::INTEGER(1), Value::INTEGER(1), Value::INTEGER(2), Value::INTEGER(2),
	                                   Value::INTEGER(2), Value::INTEGER(3), Value::INTEGER(5)})}));

	result = con.Query("SELECT LIST(42)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::LIST({Value::INTEGER(42)})}));

	result = con.Query("SELECT LIST(42) FROM list_data");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::LIST({Value::INTEGER(42), Value::INTEGER(42), Value::INTEGER(42), Value::INTEGER(42),
	                                   Value::INTEGER(42), Value::INTEGER(42), Value::INTEGER(42)})}));

	result = con.Query("SELECT UNNEST(LIST(42))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// unlist is alias of unnest for symmetry reasons
	result = con.Query("SELECT UNLIST(LIST(42))");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("SELECT UNNEST(LIST(e)) ue, UNNEST(LIST(g)) ug from list_data ORDER BY ue");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 2, 3, 4, 5, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 1, 1, 2, 2, 2, 3}));

	result = con.Query("SELECT g, UNNEST(LIST(e)) ue, UNNEST(LIST(e+1)) ue2 from list_data GROUP BY g ORDER BY ue");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 1, 1, 2, 2, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3, 4, 5, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), 2, 3, 4, 5, 6, 7}));

	result = con.Query("SELECT g, UNNEST(l) u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1 ORDER BY u");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 1, 1, 2, 2, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3, 4, 5, 6}));

	result = con.Query("SELECT g, UNNEST(l)+1 u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1 ORDER BY u");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 1, 1, 2, 2, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 3, 4, 5, 6, 7}));

	// omg omg, list of structs, structs of lists

	result =
	    con.Query("SELECT g, STRUCT_PACK(a := g, b := le) sl FROM (SELECT g, LIST(e) le from list_data GROUP BY g) "
	              "xx WHERE g < 3 ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(
	    result, 1,
	    {Value::STRUCT(
	         {make_pair("a", Value::INTEGER(1)), make_pair("b", Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}))}),
	     Value::STRUCT({make_pair("a", Value::INTEGER(2)),
	                    make_pair("b", Value::LIST({Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5)}))})}));

	result = con.Query("SELECT LIST(STRUCT_PACK(a := g, b := le)) mind_blown FROM (SELECT g, LIST(e) le from list_data "
	                   " GROUP BY g ORDER BY g) xx");

	REQUIRE(CHECK_COLUMN(
	    result, 0,
	    {Value::LIST(
	        {Value::STRUCT({make_pair("a", Value::INTEGER(1)),
	                        make_pair("b", Value::LIST({Value::INTEGER(1), Value::INTEGER(2)}))}),
	         Value::STRUCT({make_pair("a", Value::INTEGER(2)),
	                        make_pair("b", Value::LIST({Value::INTEGER(3), Value::INTEGER(4), Value::INTEGER(5)}))}),
	         Value::STRUCT({make_pair("a", Value::INTEGER(3)), make_pair("b", Value::LIST({Value::INTEGER(6)}))}),
	         Value::STRUCT({make_pair("a", Value::INTEGER(5)), make_pair("b", Value::LIST({Value()}))})})}));

	result = con.Query("SELECT g, LIST(STRUCT_PACK(a := e, b := e+1)) ls from list_data GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 5}));
	// TODO check second col

	result =
	    con.Query("SELECT g, LIST(STRUCT_PACK(a := e, b := e+1)) ls from list_data WHERE g > 2GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 5}));
	REQUIRE(CHECK_COLUMN(
	    result, 1,
	    {Value::LIST({Value::STRUCT({make_pair("a", Value::INTEGER(6)), make_pair("b", Value::INTEGER(7))})}),
	     Value::LIST({Value::STRUCT({make_pair("a", Value()), make_pair("b", Value())})})}));

	// list of list of int
	result = con.Query(
	    "SELECT g2, LIST(le) FROM (SELECT g % 2 g2, LIST(e) le from list_data GROUP BY g ORDER BY g) sq 	GROUP "
	    "BY g2 ORDER BY g2");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));

	REQUIRE(CHECK_COLUMN(result, 1,
	                     {Value::LIST({Value::LIST({3, 4, 5})}),
	                      Value::LIST({Value::LIST({1, 2}), Value::LIST({6}), Value::LIST({Value()})})}));

	result = con.Query("SELECT SUM(ue) FROM (SELECT UNNEST(le) ue FROM (SELECT g, LIST(e) le from list_data "
	                   " GROUP BY g ORDER BY g) xx) xy");
	REQUIRE(CHECK_COLUMN(result, 0, {21}));
	// this is technically equivalent but is not supported
	REQUIRE_FAIL(con.Query("SELECT SUM(UNNEST(le)) FROM ( SELECT g, LIST(e) le from list_data "
	                       " GROUP BY g ORDER BY g) xx"));

	// you're holding it wrong
	REQUIRE_FAIL(con.Query("SELECT LIST(LIST(42))"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST(UNNEST(LIST(42))"));

	REQUIRE_FAIL(con.Query("SELECT LIST()"));
	REQUIRE_FAIL(con.Query("SELECT LIST() FROM list_data"));
	REQUIRE_FAIL(con.Query("SELECT LIST(e, g) FROM list_data"));

	REQUIRE_FAIL(con.Query("SELECT g, UNNEST(l+1) u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1"));
	REQUIRE_FAIL(con.Query("SELECT g, UNNEST(g) u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1"));
	REQUIRE_FAIL(con.Query("SELECT g, UNNEST() u FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1"));

	REQUIRE_FAIL(con.Query("SELECT UNNEST(42)"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST()"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST(42) from list_data"));
	REQUIRE_FAIL(con.Query("SELECT UNNEST() from list_data"));
	REQUIRE_FAIL(con.Query("SELECT g FROM (SELECT g, LIST(e) l FROM list_data GROUP BY g) u1 where UNNEST(l) > 42"));
}

TEST_CASE("Test packing and unpacking lineitem into lists", "[nested][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<MaterializedQueryResult> result;
	con.EnableQueryVerification(); // FIXME something odd happening here
	auto sf = 0.01;
	// TODO this has a small limit in it right now because of performance issues. Fix this.
	tpch::dbgen(sf, db, DEFAULT_SCHEMA, "_org");

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE lineitem_small AS SELECT * FROM lineitem_org LIMIT 1050;"));

	REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE VIEW lineitem AS SELECT * FROM lineitem_small"));
	// run the regular Q1 on the small lineitem set
	result = con.Query(tpch::get_query(1));
	// construct the expected values from the regular query
	vector<vector<Value>> expected_values;
	for (idx_t col_idx = 0; col_idx < result->sql_types.size(); col_idx++) {
		vector<Value> column_list;
		for (idx_t row_idx = 0; row_idx < result->collection.count; row_idx++) {
			column_list.push_back(result->GetValue(col_idx, row_idx));
		}
		expected_values.push_back(column_list);
	}

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE OR REPLACE VIEW lineitem AS SELECT l_orderkey, STRUCT_EXTRACT(struct, 'l_partkey') l_partkey, "
	    "STRUCT_EXTRACT(struct, 'l_suppkey') l_suppkey, STRUCT_EXTRACT(struct, 'l_linenumber') l_linenumber, "
	    "STRUCT_EXTRACT(struct, 'l_quantity') l_quantity, STRUCT_EXTRACT(struct, 'l_extendedprice') l_extendedprice, "
	    "STRUCT_EXTRACT(struct, 'l_discount') l_discount, STRUCT_EXTRACT(struct, 'l_tax') l_tax, "
	    "STRUCT_EXTRACT(struct, 'l_returnflag') l_returnflag, STRUCT_EXTRACT(struct, 'l_linestatus') l_linestatus, "
	    "STRUCT_EXTRACT(struct, 'l_shipdate') l_shipdate, STRUCT_EXTRACT(struct, 'l_commitdate') l_commitdate, "
	    "STRUCT_EXTRACT(struct, 'l_receiptdate') l_receiptdate, STRUCT_EXTRACT(struct, 'l_shipinstruct') "
	    "l_shipinstruct, STRUCT_EXTRACT(struct, 'l_shipmode') l_shipmode, STRUCT_EXTRACT(struct, 'l_comment') "
	    "l_comment FROM (SELECT l_orderkey, UNLIST(rest) struct FROM (SELECT l_orderkey, LIST(STRUCT_PACK(l_partkey "
	    ",l_suppkey ,l_linenumber ,l_quantity ,l_extendedprice ,l_discount ,l_tax ,l_returnflag ,l_linestatus "
	    ",l_shipdate ,l_commitdate ,l_receiptdate ,l_shipinstruct ,l_shipmode ,l_comment)) rest FROM (SELECT * FROM "
	    "lineitem_small ) lss GROUP BY l_orderkey) s1) s2;"));
	result = con.Query(tpch::get_query(1));
	for (idx_t col_idx = 0; col_idx < expected_values.size(); col_idx++) {
		REQUIRE(CHECK_COLUMN(result, col_idx, expected_values[col_idx]));
	}

	// database as-a-value
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE OR REPLACE VIEW lineitem AS SELECT STRUCT_EXTRACT(ls, 'l_orderkey') l_orderkey, STRUCT_EXTRACT(ls, "
	    "'l_partkey') l_partkey, STRUCT_EXTRACT(ls, 'l_suppkey') l_suppkey, STRUCT_EXTRACT(ls, 'l_linenumber') "
	    "l_linenumber, STRUCT_EXTRACT(ls, 'l_quantity') l_quantity, STRUCT_EXTRACT(ls, 'l_extendedprice') "
	    "l_extendedprice, STRUCT_EXTRACT(ls, 'l_discount') l_discount, STRUCT_EXTRACT(ls, 'l_tax') l_tax, "
	    "STRUCT_EXTRACT(ls, 'l_returnflag') l_returnflag, STRUCT_EXTRACT(ls, 'l_linestatus') l_linestatus, "
	    "STRUCT_EXTRACT(ls, 'l_shipdate') l_shipdate, STRUCT_EXTRACT(ls, 'l_commitdate') l_commitdate, "
	    "STRUCT_EXTRACT(ls, 'l_receiptdate') l_receiptdate, STRUCT_EXTRACT(ls, 'l_shipinstruct') l_shipinstruct, "
	    "STRUCT_EXTRACT(ls, 'l_shipmode') l_shipmode, STRUCT_EXTRACT(ls, 'l_comment') l_comment FROM (SELECT "
	    "UNNEST(lineitem) ls FROM (SELECT LIST(STRUCT_PACK(l_orderkey, l_partkey ,l_suppkey ,l_linenumber ,l_quantity "
	    ",l_extendedprice ,l_discount ,l_tax ,l_returnflag ,l_linestatus ,l_shipdate ,l_commitdate ,l_receiptdate "
	    ",l_shipinstruct ,l_shipmode ,l_comment)) lineitem FROM (SELECT * FROM lineitem_small) s1) s2) s3;"));
	result = con.Query(tpch::get_query(1));
	for (idx_t col_idx = 0; col_idx < expected_values.size(); col_idx++) {
		REQUIRE(CHECK_COLUMN(result, col_idx, expected_values[col_idx]));
	}

	REQUIRE_NO_FAIL(con.Query(
	    "CREATE OR REPLACE VIEW lineitem AS SELECT UNNEST(STRUCT_EXTRACT(lineitem, 'll_orderkey')) l_orderkey, "
	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_partkey')) l_partkey, UNNEST(STRUCT_EXTRACT(lineitem, 'll_suppkey')) "
	    "l_suppkey, UNNEST(STRUCT_EXTRACT(lineitem, 'll_linenumber')) l_linenumber, UNNEST(STRUCT_EXTRACT(lineitem, "
	    "'ll_quantity')) l_quantity, UNNEST(STRUCT_EXTRACT(lineitem, 'll_extendedprice')) l_extendedprice, "
	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_discount')) l_discount, UNNEST(STRUCT_EXTRACT(lineitem, 'll_tax')) l_tax, "
	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_returnflag')) l_returnflag, UNNEST(STRUCT_EXTRACT(lineitem, "
	    "'ll_linestatus')) l_linestatus, UNNEST(STRUCT_EXTRACT(lineitem, 'll_shipdate')) l_shipdate, "
	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_commitdate')) l_commitdate, UNNEST(STRUCT_EXTRACT(lineitem, "
	    "'ll_receiptdate')) l_receiptdate, UNNEST(STRUCT_EXTRACT(lineitem, 'll_shipinstruct')) l_shipinstruct, "
	    "UNNEST(STRUCT_EXTRACT(lineitem, 'll_shipmode')) l_shipmode, UNNEST(STRUCT_EXTRACT(lineitem, 'll_comment')) "
	    "l_comment FROM (SELECT STRUCT_PACK(ll_orderkey:= LIST(l_orderkey), ll_partkey := LIST(l_partkey ), ll_suppkey "
	    ":= LIST(l_suppkey ), ll_linenumber := LIST(l_linenumber ), ll_quantity := LIST(l_quantity ), ll_extendedprice "
	    ":= LIST(l_extendedprice ), ll_discount := LIST(l_discount ), ll_tax := LIST(l_tax ), ll_returnflag := "
	    "LIST(l_returnflag ), ll_linestatus := LIST(l_linestatus ), ll_shipdate := LIST(l_shipdate ), ll_commitdate := "
	    "LIST(l_commitdate ), ll_receiptdate := LIST(l_receiptdate ), ll_shipinstruct := LIST(l_shipinstruct ), "
	    "ll_shipmode := LIST(l_shipmode ), ll_comment:= LIST(l_comment)) lineitem FROM (SELECT * FROM lineitem_small) "
	    "s1) s2;"));
	result = con.Query(tpch::get_query(1));
	for (idx_t col_idx = 0; col_idx < expected_values.size(); col_idx++) {
		REQUIRE(CHECK_COLUMN(result, col_idx, expected_values[col_idx]));
	}
}

// TEST_CASE("Aggregate lists", "[nested]") {
//	DuckDB db(nullptr);
//	Connection con(db);
//	con.EnableQueryVerification();
//	unique_ptr<QueryResult> result;
//
//	result = con.Query("SELECT SUM(a), b FROM (VALUES (42, LIST_VALUE(1, 2)), (42, LIST_VALUE(3, 4, 5)), (24,
// LIST_VALUE(1, 2))) lv(a, b) GROUP BY b"); 	result->Print();
//}
