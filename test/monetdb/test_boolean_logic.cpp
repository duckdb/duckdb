#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("MonetDB Test: boolean_not.Bug-3505.sql", "[monetdb]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE BOOLTBL1 (f1 bool);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO BOOLTBL1 (f1) VALUES (cast('true' AS boolean));"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO BOOLTBL1 (f1) VALUES ('true');"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO BOOLTBL1 (f1) VALUES ('false');"));

	result = con.Query("SELECT f1 FROM BOOLTBL1 WHERE f1 = NOT FALSE;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true}));
	result = con.Query("SELECT f1 FROM BOOLTBL1 WHERE f1 = NOT TRUE;");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));

	result = con.Query("SELECT f1 FROM BOOLTBL1 WHERE f1 = (NOT FALSE);");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true}));
	result = con.Query("SELECT f1 FROM BOOLTBL1 WHERE NOT FALSE = f1;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true}));
	result = con.Query("SELECT f1 FROM BOOLTBL1 WHERE NOT f1 = FALSE;");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true}));

	result = con.Query("SELECT f1 FROM BOOLTBL1 WHERE f1 = (NOT TRUE);");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT f1 FROM BOOLTBL1 WHERE NOT TRUE = f1;");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
	result = con.Query("SELECT f1 FROM BOOLTBL1 WHERE NOT f1 = TRUE;");
	REQUIRE(CHECK_COLUMN(result, 0, {false}));
}
