
#include "catch.hpp"
#include "../tpch/test_helpers.hpp"

using namespace duckdb;
using namespace std;

// FIXME: this needs to go somewhere else, but not sure where, the helpers are
// here
TEST_CASE("BETWEEN", "[sql]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("CREATE TABLE between_tests (a INTEGER)");
	con.Query("INSERT INTO between_tests VALUES (42)");
	con.Query("INSERT INTO between_tests VALUES (43)");
	con.Query("INSERT INTO between_tests VALUES (44)");
	con.Query("INSERT INTO between_tests VALUES (45)");
	result = con.Query("SELECT * FROM between_tests");

	CHECK_COLUMN(result, 0, {42, 43, 44, 45});

	result = con.Query("SELECT a FROM between_tests WHERE a BETWEEN 43 AND 44");

	CHECK_COLUMN(result, 0, {43, 44});

	result =
	    con.Query("SELECT a FROM between_tests WHERE a NOT BETWEEN 43 AND 44");

	CHECK_COLUMN(result, 0, {42, 45});
}
