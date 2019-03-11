#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test concat", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello'), ('world'), (NULL)"));
	
	result = con.Query("SELECT s || ' ' || s FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello hello", "world world"}));

	// unicode concat
	result = con.Query("SELECT s || ' ' || '🦆' FROM strings ORDER BY s");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), "hello 🦆", "world 🦆"}));
}
