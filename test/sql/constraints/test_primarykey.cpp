#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("PRIMARY KEY prefix stress test multiple columns", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.AddComment("create a table");
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b VARCHAR, PRIMARY KEY(a, b));"));

	con.AddComment("Insert 300 values");
	for (idx_t idx = 0; idx < 300; idx++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(idx) + ", 'hello_" + to_string(idx) + "')"));
	}

	con.AddComment("Inserting same values should fail");
	for (idx_t idx = 0; idx < 300; idx++) {
		REQUIRE_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(idx) + ", 'hello_" + to_string(idx) + "')"));
	}

	con.AddComment("Update integer a on 1000 should work since there are no duplicates");
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=a+1000;"));

	con.AddComment("Now inserting same 1000 values should work");
	for (idx_t idx = 0; idx < 300; idx++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (" + to_string(idx) + ", 'hello_" + to_string(idx) + "')"));
	}

	con.AddComment("This update should fail and stress test the deletes on hello_ prefixes");
	REQUIRE_FAIL(con.Query("UPDATE test SET a=a+1000;"));

	con.AddComment("Should fail for same reason as above, just checking element per element to see if no one is escaping");
	for (idx_t idx = 0; idx < 300; idx++) {
		REQUIRE_FAIL(
		    con.Query("INSERT INTO test VALUES (" + to_string(idx + 1000) + ", 'hello_" + to_string(idx) + "')"));
	}
}

TEST_CASE("Test appending the same value many times to a primary key column", "[constraints]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY)");
	con.AddComment("insert a bunch of values into the index and query the index");
	for (int32_t val = 0; val < 100; val++) {
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {0}));

		con.Query("INSERT INTO integers VALUES ($1)", val);

		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	for (int32_t val = 0; val < 100; val++) {
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i + i = " + to_string(val) + "+" + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	con.AddComment("now insert the same values, this should fail this time");
	for (int32_t it = 0; it < 10; it++) {
		int32_t val = 64;
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i + i = 64+" + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("SELECT COUNT(*) FROM integers WHERE i = " + to_string(val));
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("INSERT INTO integers VALUES ($1)", val);
		REQUIRE_FAIL(result);
	}

	con.AddComment("now test that the counts are correct");
	result = con.Query("SELECT COUNT(*), COUNT(DISTINCT i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {100}));
	REQUIRE(CHECK_COLUMN(result, 1, {100}));
}
