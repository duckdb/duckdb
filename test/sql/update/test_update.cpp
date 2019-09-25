#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test standard update behavior", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	// create a table
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (3)"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// test simple update
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=1"));

	// not seen yet by con2, only by con1
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	// commit
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// now test a rollback
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET a=4"));

	// not seen yet by con2, only by con1
	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	result = con2.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	result = con.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con2.Query("SELECT * FROM test");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test update behavior with multiple updaters", "[update]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db), con2(db), con3(db), con4(db);
	Connection u1(db), u2(db), u3(db);

	// create a table, filled with 3 values (1), (2), (3)
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (1), (2), (3)"));

	// now we start updating specific values and reading different versions
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(u1.Query("UPDATE test SET a=4 WHERE a=1"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(u2.Query("UPDATE test SET a=5 WHERE a=2"));
	REQUIRE_NO_FAIL(con3.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(u3.Query("UPDATE test SET a=6 WHERE a=3"));
	REQUIRE_NO_FAIL(con4.Query("BEGIN TRANSACTION"));

	// now read the different states
	// con sees {1, 2, 3}
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	// con2 sees {2, 3, 4}
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 4}));
	// con3 sees {3, 4, 5}
	result = con3.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 5}));
	// con4 sees {4, 5, 6}
	result = con4.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5, 6}));

	// now verify that we get conflicts when we update values that have been updated AFTER we started
	REQUIRE_FAIL(con.Query("UPDATE test SET a=99 WHERE a=1"));
	REQUIRE_FAIL(con2.Query("UPDATE test SET a=99 WHERE a=2"));
	REQUIRE_FAIL(con3.Query("UPDATE test SET a=99 WHERE a=3"));

	// however we CAN update values that were committed BEFORE we started
	REQUIRE_NO_FAIL(con2.Query("UPDATE test SET a=7 WHERE a=4"));
	REQUIRE_NO_FAIL(con3.Query("UPDATE test SET a=8 WHERE a=5"));
	REQUIRE_NO_FAIL(con4.Query("UPDATE test SET a=9 WHERE a=6"));

	// now read the different states again
	// con sees {1, 2, 3} still
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	// con2 sees {2, 3, 7}
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 7}));
	// con3 sees {3, 4, 8}
	result = con3.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 8}));
	// con4 sees {4, 5, 9}
	result = con4.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5, 9}));
	// u1 still sees {4, 5, 6}
	result = u1.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5, 6}));

	// now we commit in phases
	// first we commit con4
	REQUIRE_NO_FAIL(con4.Query("COMMIT"));

	// con, con2, con3 still see the same data, con4 sees the currently committed data
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 7}));
	result = con3.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 8}));
	result = con4.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {4, 5, 9}));

	// then we commit con2
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// con, con3 still see the same data, con2 and con4 see the committed data
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 7, 9}));
	result = con3.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {3, 4, 8}));
	result = con4.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 7, 9}));

	// then we commit con3
	REQUIRE_NO_FAIL(con3.Query("COMMIT"));

	// con still sees the same data, but the rest all see the committed data
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	result = con2.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {7, 8, 9}));
	result = con3.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {7, 8, 9}));
	result = con4.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {7, 8, 9}));

	// now we commit con1, this should trigger a cleanup
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	// now con also sees the committed data
	result = con.Query("SELECT * FROM test ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {7, 8, 9}));
}
