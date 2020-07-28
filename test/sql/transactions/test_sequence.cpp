#include "catch.hpp"
#include "test_helpers.hpp"

#include <algorithm>
#include <mutex>
#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test Sequences Are Transactional", "[sequence]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	Connection con2(db);

	// start a transaction for both nodes
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));

	// create a sequence in node one
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	// node one can see it
	REQUIRE_NO_FAIL(con.Query("SELECT nextval('seq');"));
	// node two can't see it
	REQUIRE_FAIL(con2.Query("SELECT nextval('seq');"));

	// we commit the sequence
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
	// node two still can't see it
	REQUIRE_FAIL(con2.Query("SELECT nextval('seq');"));
	// now commit node two
	REQUIRE_NO_FAIL(con2.Query("COMMIT"));

	// we can now see the sequence in node two
	REQUIRE_NO_FAIL(con2.Query("SELECT nextval('seq');"));

	// drop sequence seq in a transaction
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// node one can't use it anymore
	REQUIRE_FAIL(con.Query("SELECT nextval('seq');"));
	// node two can still use it
	REQUIRE_NO_FAIL(con2.Query("SELECT nextval('seq');"));

	// rollback cancels the drop sequence
	REQUIRE_NO_FAIL(con.Query("ROLLBACK;"));

	// we can still use it
	REQUIRE_NO_FAIL(con2.Query("SELECT nextval('seq');"));

	// now we drop it for real
	REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));

	// we can't use it anymore
	REQUIRE_FAIL(con.Query("SELECT nextval('seq');"));
	REQUIRE_FAIL(con2.Query("SELECT nextval('seq');"));
}
