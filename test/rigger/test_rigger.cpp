#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Tests found by Rigger", "[rigger]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

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
	SECTION("495") {
		// Comparison on UNIQUE NUMERIC column causes a query to omit a row in the result set
		// REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 NUMERIC UNIQUE);"));
		// REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (1163404482), (0), (488566);"));
		// result = con.Query("SELECT * FROM t0 WHERE c0 > 0.1 ORDER BY 1;");
		// REQUIRE(CHECK_COLUMN(result, 0, {488566, 1163404482}));
		// result = con.Query("SELECT * FROM t0 WHERE c0 >= 0.1 ORDER BY 1;");
		// REQUIRE(CHECK_COLUMN(result, 0, {488566, 1163404482}));
		// result = con.Query("SELECT * FROM t0 WHERE 0.1 < c0 ORDER BY 1;");
		// REQUIRE(CHECK_COLUMN(result, 0, {488566, 1163404482}));
		// result = con.Query("SELECT * FROM t0 WHERE 0.1 <= c0 ORDER BY 1;");
		// REQUIRE(CHECK_COLUMN(result, 0, {488566, 1163404482}));
	}
	SECTION("497") {
		// Comparison of two boolean columns in different tables results in an error "Not implemented: Unimplemented type for sort"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 BOOL);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 BOOL);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT t0.c0 FROM t0, t1 WHERE t1.c0 < t0.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("503") {
		// RIGHT JOIN with a predicate that compares two integer columns results in an "Unhandled type" error
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		result = con.Query("SELECT * FROM t0 RIGHT JOIN t1 ON t0.c0!=t1.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		REQUIRE(CHECK_COLUMN(result, 1, {}));
	}
	SECTION("504") {
		// INSERT results in an error "Not implemented: Cannot create data from this type"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 BOOLEAN, c1 INT, PRIMARY KEY(c0, c1));"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c1, c0) VALUES (0, 0);"));
		result = con.Query("SELECT * FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		REQUIRE(CHECK_COLUMN(result, 1, {0}));
	}
	SECTION("505") {
		// A RIGHT JOIN unexpectedly fetches rows
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c1 BOOLEAN);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (1);"));
		result = con.Query("SELECT * FROM t0 RIGHT JOIN t1 on true;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		REQUIRE(CHECK_COLUMN(result, 1, {}));
	}
	SECTION("506") {
		// Query results in an error "INTERNAL: Failed to bind column reference "c0" [5.0] (bindings: [6.0])"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM t1 JOIN t0 ON t1.c0 < t1.c0 - t0.c0 WHERE t0.c0 <= t1.c0;"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM t1 JOIN t0 ON t0.c0 + t1.c0 < t1.c0 - t0.c0;"));
	}
	SECTION("507") {
		// Creating an empty table results in a crash
		REQUIRE_FAIL(con.Query("CREATE TABLE t0();"));
	}
	SECTION("508") {
		// LEFT JOIN on column with NULL value results in a segmentation fault
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (NULL);"));
		result = con.Query("SELECT * FROM t1 LEFT JOIN t0 ON t0.c0=t1.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	}
	SECTION("510") {
		// SIMILAR TO results in an incorrect result
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (-10);"));
		result = con.Query("SELECT '-10' SIMILAR TO '0';");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		result = con.Query("SELECT t0.c0 SIMILAR TO 0 FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		result = con.Query("SELECT t0.c0 NOT SIMILAR TO 0 FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 NOT SIMILAR TO 0;");
		REQUIRE(CHECK_COLUMN(result, 0, {-10}));
	}


}
