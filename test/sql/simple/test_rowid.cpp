#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Row IDs", "[rowid]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table a(i integer);"));
	REQUIRE_NO_FAIL(con.Query("insert into a values (42);"));

	// we can query row ids
	result = con.Query("SELECT rowid, * FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {42}));
	REQUIRE(result->types.size() == 2);

	// rowid isn't expanded in *
	result = con.Query("SELECT * FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(result->types.size() == 1);

	// we can't update rowids
	REQUIRE_FAIL(con.Query("UPDATE a SET rowid=5"));
	REQUIRE_FAIL(con.Query("UPDATE a SET _duckdb_internal_rowid=5"));
	// we also can't insert with explicit row ids
	REQUIRE_FAIL(con.Query("INSERT INTO a (rowid, i)  VALUES (5, 6)"));

	// we can use rowid as column name
	REQUIRE_NO_FAIL(con.Query("create table b(rowid integer);"));
	REQUIRE_NO_FAIL(con.Query("insert into b values (42), (22);"));

	// this rowid is expanded
	result = con.Query("SELECT * FROM b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 42}));
	REQUIRE(result->types.size() == 1);

	// selecting rowid just selects the column
	result = con.Query("SELECT rowid FROM b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 42}));
	REQUIRE(result->types.size() == 1);
	// now we can update
	REQUIRE_NO_FAIL(con.Query("UPDATE a SET rowid=5"));
	// and insert
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a (rowid) VALUES (5)"));

	result = con.Query("SELECT * FROM b");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 5, 5}));

	// we can still select the internal rowid
	result = con.Query("SELECT _duckdb_internal_rowid, rowid FROM b");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {5, 5, 5}));



}
