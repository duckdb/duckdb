#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

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
	index_t tpl_count = 2 * Storage::BLOCK_SIZE / sizeof(int);
	auto prepared = con2.Prepare("INSERT INTO integers VALUES (?)");
	for(int i = 2; i < tpl_count; i++) {
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

	result = con.Query("SELECT * FROM integers ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
}
