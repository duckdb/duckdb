#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test that index entries are properly removed after aborted append", "[transactions][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY);"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));
	// insert the values [2..2048] into the table
	for (int i = 2; i <= 2048; i++) {
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (" + to_string(i) + ");"));
	}
	REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (" + to_string(1) + ");"));

	// con commits first
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));
	// con2 fails to commit because of the conflict
	REQUIRE_FAIL(con2.Query("COMMIT;"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// now append the rows [2..2048 again]
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));
	for (int i = 2; i <= 2048; i++) {
		REQUIRE_NO_FAIL(con2.Query("INSERT INTO integers VALUES (" + to_string(i) + ");"));
	}
	// this time the commit should work
	REQUIRE_NO_FAIL(con2.Query("COMMIT;"));

	result = con.Query("SELECT COUNT(*), MIN(i), MAX(i) FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {2048}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {2048}));
}

TEST_CASE("Test abort of big append", "[transactions][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY);"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION;"));

	// insert two blocks worth of values into the table in con2, plus the value [1]
	// and the value [1] in con
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1);"));
	idx_t tpl_count = 2 * Storage::BLOCK_SIZE / sizeof(int);
	auto prepared = con2.Prepare("INSERT INTO integers VALUES (?)");
	for (int i = 2; i < (int32_t)tpl_count; i++) {
		REQUIRE_NO_FAIL(prepared->Execute(i));
	}
	// finally insert the value "1"
	REQUIRE_NO_FAIL(prepared->Execute(1));

	// con commits first
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));
	// con2 fails to commit because of the conflict
	REQUIRE_FAIL(con2.Query("COMMIT;"));

	result = con.Query("SELECT * FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// now append some rows again
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (2);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (4);"));

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
}
