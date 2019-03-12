#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test unicode strings", "[unicode]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// insert unicode strings into the database
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE emojis(id INTEGER, s VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO emojis VALUES (1, '🦆'), (2, '🦆🍞🦆')"));

	// retrieve unicode strings again
	result = con.Query("SELECT * FROM emojis ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	REQUIRE(CHECK_COLUMN(result, 1, {"🦆", "🦆🍞🦆"}));

	// substring on emojis
	result = con.Query("SELECT substring(s, 1, 1), substring(s, 2, 1) FROM emojis ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {"🦆", "🦆"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"", "🍞"}));

	// length on emojis
	result = con.Query("SELECT length(s) FROM emojis ORDER BY id");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 3}));
}
