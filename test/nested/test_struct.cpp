#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test filter and projection of nested struct", "[nested]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE struct_data (g INTEGER, e INTEGER)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO struct_data VALUES (1, 1), (1, 2), (2, 3), (2, 4), (2, 5), (3, 6), (5, NULL)"));

	// all the wrong ways of holding this
	REQUIRE_FAIL(con.Query("SELECT STRUCT_PACK() FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_PACK(e+1) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_PACK(a := e, a := g) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_PACK(e, e) FROM struct_data"));

	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(e, 'e') FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(e) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT('e') FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT() FROM struct_data"));

	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), 'zz') FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g)) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), g) FROM struct_data"));
	REQUIRE_FAIL(con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), '42) FROM struct_data"));

	REQUIRE_FAIL(con.Query("CREATE TABLE test AS SELECT e, STRUCT_PACK(e) FROM struct_data"));

	result = con.Query("SELECT STRUCT_PACK(a := 42, b := 43)");
	REQUIRE(CHECK_COLUMN(result, 0,
	                     {Value::STRUCT({make_pair("a", Value::INTEGER(42)), make_pair("b", Value::INTEGER(43))})}));

	result = con.Query("SELECT e, STRUCT_PACK(e) FROM struct_data ORDER BY e LIMIT 2");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1}));
	REQUIRE(CHECK_COLUMN(
	    result, 1, {Value::STRUCT({make_pair("e", Value())}), Value::STRUCT({make_pair("e", Value::INTEGER(1))})}));

	result = con.Query("SELECT e, STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), 'xx') as ee FROM struct_data");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5, 6, Value()}));

	result =
	    con.Query("SELECT e, STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), 'xx') as s FROM struct_data WHERE e > 4");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 6}));

	result = con.Query(
	    "SELECT e, STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), 'xx') as s FROM struct_data WHERE e IS NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	result = con.Query("SELECT e, STRUCT_EXTRACT(STRUCT_PACK(xx := e/2), 'xx') as s FROM struct_data WHERE e > 4");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 3}));

	result = con.Query("SELECT e, STRUCT_EXTRACT(STRUCT_PACK(xx := e/2), 'xx')*2 as s FROM struct_data WHERE e > 4");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 6}));
	REQUIRE(CHECK_COLUMN(result, 1, {4, 6}));

	result = con.Query(
	    "SELECT e, STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), 'xx') as ee FROM struct_data ORDER BY e DESC");
	REQUIRE(CHECK_COLUMN(result, 0, {6, 5, 4, 3, 2, 1, Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {6, 5, 4, 3, 2, 1, Value()}));

	result = con.Query("SELECT e, STRUCT_EXTRACT(STRUCT_PACK(a := e, b := ROWID, c := 42), 'c') as ee FROM struct_data "
	                   "ORDER BY ROWID");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4, 5, 6, Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 42, 42, 42, 42, 42, 42}));

	result = con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(a := 42, b := 43), 'a') FROM struct_data");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 42, 42, 42, 42, 42, 42}));

	result = con.Query("SELECT STRUCT_EXTRACT(STRUCT_PACK(a := 42, b := 43), 'a') s");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("SELECT STRUCT_EXTRACT(STRUCT_EXTRACT(STRUCT_PACK(a := STRUCT_PACK(x := 'asdf', y := NULL), b "
	                   ":= 43), 'a'), 'x') s");
	REQUIRE(CHECK_COLUMN(result, 0, {"asdf"}));
}
