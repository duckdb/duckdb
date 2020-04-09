#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Tests found by Rigger", "[rigger]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	// con.EnableQueryVerification();

	SECTION("489") {
		// A predicate NOT(NULL OR TRUE) unexpectedly evaluates to TRUE
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT * FROM t0 WHERE NOT(NULL OR TRUE);");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		result = con.Query("SELECT NULL OR TRUE;");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT NOT(NULL OR TRUE);");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
	}
	SECTION("490") {
		// A comparison column=column unexpectedly evaluates to TRUE for column=NULL
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (NULL);"));
		result = con.Query("SELECT * FROM t0 WHERE c0 = c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("491") {
		// PRAGMA table_info provides no output
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		result = con.Query("PRAGMA table_info('t0');");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {"c0"}));
		REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER"}));
		REQUIRE(CHECK_COLUMN(result, 3, {false}));
		REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 5, {false}));
		result = con.Query("SELECT * FROM pragma_table_info('t0');");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {"c0"}));
		REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER"}));
		REQUIRE(CHECK_COLUMN(result, 3, {false}));
		REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 5, {false}));
	}
	SECTION("493") {
		// SIMILAR TO results in an "Unknown error -1
		result = con.Query("SELECT '' SIMILAR TO '';");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}

}
