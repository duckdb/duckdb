#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("PREPARE for SELECT", "[prepared]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);

	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("PREPARE s1 AS SELECT CAST($1 AS INTEGER), CAST($2 AS STRING)"));
	result = con.Query("EXECUTE s1(42, 'dpfkg')");
	REQUIRE(result->GetSuccess());
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {"dpfkg"}));

	result = con.Query("EXECUTE s1(43, 'asdf')");
	REQUIRE(result->GetSuccess());
	REQUIRE(CHECK_COLUMN(result, 0, {43}));
	REQUIRE(CHECK_COLUMN(result, 1, {"asdf"}));

	// not enough params
	REQUIRE_FAIL(con.Query("EXECUTE s1(43)"));
	// too many
	REQUIRE_FAIL(con.Query("EXECUTE s1(43, 'asdf', 42)"));
	// wrong non-castable types
	REQUIRE_FAIL(con.Query("EXECUTE s1('asdf', 'asdf')"));

	REQUIRE_NO_FAIL(con.Query("DEALLOCATE s1"));

	// we can deallocate non-existing statements
	REQUIRE_NO_FAIL(con.Query("DEALLOCATE s2"));

	// now its gone
	REQUIRE_FAIL(con.Query("EXECUTE s1(42, 'dpfkg')"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a (i TINYINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (42)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE s3 AS SELECT * FROM a WHERE i=$1"));

	REQUIRE_FAIL(con.Query("EXECUTE s3(10000)"));

	result = con.Query("EXECUTE s3(42)");
	REQUIRE(result->GetSuccess());
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("EXECUTE s3(84)");
	REQUIRE(result->GetSuccess());
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	REQUIRE_NO_FAIL(con.Query("DEALLOCATE s3"));

	// can't run a query with a param without PREPARE
	REQUIRE_FAIL(con.Query("SELECT * FROM a WHERE i=$1"));
}

TEST_CASE("PREPARE for INSERT", "[prepared]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE b (i TINYINT)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE s1 AS INSERT INTO b VALUES (cast($1 as tinyint)), ($2 + 1), ($3)"));
	REQUIRE_NO_FAIL(con.Query("EXECUTE s1 (42, 41, 42)"));

	result = con.Query("SELECT * FROM b");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 42, 42}));
	REQUIRE_FAIL(con.Query("EXECUTE s1 (42, 41, 10000)"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE c (i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE s2 AS INSERT INTO c VALUES ($1)"));

	for (size_t i = 0; i < 1000; i++) {
		REQUIRE_NO_FAIL(con.Query("EXECUTE s2(" + to_string(i) + ")"));
	}

	result = con.Query("SELECT COUNT(*), MIN(i), MAX(i) FROM c");
	REQUIRE(CHECK_COLUMN(result, 0, {1000}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {999}));

	// can't drop table because we still have a prepared statement on it
	REQUIRE_FAIL(con.Query("DROP TABLE b"));
	REQUIRE_FAIL(con.Query("DROP TABLE c"));

	// TODO also try this in different connections and transaction contexts
	REQUIRE_NO_FAIL(con.Query("DEALLOCATE s2"));
	REQUIRE_NO_FAIL(con.Query("DEALLOCATE s1"));

	// now we can
	REQUIRE_NO_FAIL(con.Query("DROP TABLE b"));
	REQUIRE_NO_FAIL(con.Query("DROP TABLE c"));
}

TEST_CASE("PREPARE and DROPping tables", "[prepared]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con1(db);
	DuckDBConnection con2(db);

	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE a (i TINYINT)"));
	REQUIRE_NO_FAIL(con2.Query("PREPARE p1 AS SELECT * FROM a"));

	REQUIRE_NO_FAIL(con2.Query("EXECUTE p1"));

	// only the conn which did the prepare can execute
	REQUIRE_FAIL(con1.Query("EXECUTE p1"));

	// but someone else cannot drop the table
	REQUIRE_FAIL(con1.Query("DROP TABLE a"));

	// but when we take the statement away
	REQUIRE_NO_FAIL(con2.Query("DEALLOCATE p1"));

	// we can drop
	REQUIRE_NO_FAIL(con1.Query("DROP TABLE a"));
}

// TODO NULL
// TODO not-set param throws err
