#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/storage/storage_info.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test big append", "[append][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// append entries bigger than one block
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	idx_t desired_count = (Storage::BLOCK_SIZE * 2) / sizeof(int);
	idx_t current_count = 4;
	while (current_count < desired_count) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO integers SELECT * FROM integers"));
		current_count *= 2;
	}

	result = con.Query("SELECT COUNT(*), COUNT(i), SUM(i) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(current_count)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(current_count / 4 * 3)}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value::BIGINT(current_count / 4 * 6)}));
}
