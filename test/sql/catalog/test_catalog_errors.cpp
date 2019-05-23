#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test various errors in catalog management", "[catalog]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// create a table called integers
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	// create a view called vintegers
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW vintegers AS SELECT 42;"));

	// cannot replace table with CREATE OR REPLACE VIEW
	REQUIRE_FAIL(con.Query("CREATE OR REPLACE VIEW integers AS SELECT 42;"));
	// cannot drop a table with DROP VIEW
	REQUIRE_FAIL(con.Query("DROP VIEW integers"));

	// cannot drop a table that does not exist
	REQUIRE_FAIL(con.Query("DROP TABLE blabla"));
	// cannot alter a table that does not exist
	REQUIRE_FAIL(con.Query("ALTER TABLE blabla RENAME COLUMN i TO k"));
	// cannot drop view with DROP TABLE
	REQUIRE_FAIL(con.Query("DROP TABLE IF EXISTS vintegers"));

	// create index on i
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX i_index ON integers(i);"));
	// create the same index again, this time it already exists
	REQUIRE_FAIL(con.Query("CREATE INDEX i_index ON integers(i);"));
	// with IF NOT EXISTS it should not fail!
	REQUIRE_NO_FAIL(con.Query("CREATE INDEX IF NOT EXISTS i_index ON integers(i);"));
	// drop the index
	REQUIRE_NO_FAIL(con.Query("DROP INDEX i_index"));
	// cannot drop the index again: it no longer exists
	REQUIRE_FAIL(con.Query("DROP INDEX i_index"));
	// IF NOT EXISTS does not report a failure
	REQUIRE_NO_FAIL(con.Query("DROP INDEX IF EXISTS i_index"));
}
