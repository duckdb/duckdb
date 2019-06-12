#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

#define COLUMN_COUNT 10000

#include <fstream>

TEST_CASE("Test many columns", "[create][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	ostringstream ss;
	// many columns
	ss << "CREATE TABLE integers(";
	for (size_t i = 0; i < COLUMN_COUNT; i++) {
		ss << "i" + to_string(i) + " INTEGER, ";
	}
	ss << "j INTEGER);";

	auto query = ss.str();

	// big insert
	REQUIRE_NO_FAIL(con.Query(query));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers (i0, j) VALUES (2, 3), (3, 4), (5, 6)"));

	result = con.Query("SELECT i0, j, i1 FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 3, 5}));
	REQUIRE(CHECK_COLUMN(result, 1, {3, 4, 6}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value(), Value()}));
}
