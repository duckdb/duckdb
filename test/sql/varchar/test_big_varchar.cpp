#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Insert big varchar strings", "[varchar]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	// insert a big varchar
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('aaaaaaaaaa')"));
	// sizes: 10, 100, 1000, 10000
	for(index_t i = 0; i < 3; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a FROM test WHERE LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test)"));
	}

	result = con.Query("SELECT LENGTH(a) FROM test ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {10, 100, 1000, 10000}));
}
