#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test subqueries in update", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(id INTEGER, i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3), (4, NULL)"));

	// correlated subquery in update
	REQUIRE_NO_FAIL(con.Query("UPDATE integers i1 SET i=(SELECT MAX(i) FROM integers WHERE i1.i<>i)"));

	result = con.Query("SELECT id, i FROM integers ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3, 2, Value()}));

	// uncorrelated subquery in update
	result = con.Query("UPDATE integers i1 SET i=(SELECT MAX(i) FROM integers) WHERE i=(SELECT MIN(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT id, i FROM integers ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 3, 3, Value()}));

	// use different correlated column in subquery
	REQUIRE_NO_FAIL(con.Query("UPDATE integers i1 SET i=(SELECT MAX(id) FROM integers WHERE id<i1.id)"));

	result = con.Query("SELECT id, i FROM integers ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 2, 3}));

	// correlated subquery in WHERE
	result = con.Query("UPDATE integers i1 SET i=2 WHERE i<(SELECT MAX(id) FROM integers WHERE i1.id<id);");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	result = con.Query("SELECT id, i FROM integers ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 2, 2, 3}));

	// use DEFAULT with correlated subquery in WHERE
	result = con.Query("UPDATE integers i1 SET i=DEFAULT WHERE i=(SELECT MIN(i) FROM integers WHERE i1.id<id);");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT id, i FROM integers ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), 2, 3}));
}

TEST_CASE("Test subqueries in delete", "[subquery]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	con.EnableQueryVerification();
	con.EnableProfiling();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(id INTEGER, i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 1), (2, 2), (3, 3), (4, NULL)"));

	// correlated subquery in delete
	result = con.Query("DELETE FROM integers i1 WHERE i>(SELECT MAX(i) FROM integers WHERE i1.i<>i)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT id, i FROM integers ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, Value()}));

	// uncorrelated subquery in delete
	result = con.Query("DELETE FROM integers i1 WHERE i=(SELECT MAX(i) FROM integers)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	result = con.Query("SELECT id, i FROM integers ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, Value()}));
}
