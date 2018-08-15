#include "../tpch/test_helpers.hpp"
#include "catch.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Booleans and NULLs", "[sql]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);
	DuckDBConnection con(db);

	// AND
	result = con.Query("SELECT 0 AND 0, 0 AND 1, 1 AND 0, 1 AND 1, NULL AND "
	                   "0, NULL AND 1, 0 AND NULL, 1 AND NULL, NULL AND NULL");
	CHECK_COLUMN(result, 0, {0});
	CHECK_COLUMN(result, 1, {0});
	CHECK_COLUMN(result, 2, {0});
	CHECK_COLUMN(result, 3, {1});
	CHECK_COLUMN(result, 4, {0});
	CHECK_COLUMN(result, 5, {Value()});
	CHECK_COLUMN(result, 6, {0});
	CHECK_COLUMN(result, 7, {Value()});
	CHECK_COLUMN(result, 8, {Value()});

	// OR
	result = con.Query("SELECT 0 OR 0, 0 OR 1, 1 OR 0, 1 OR 1, NULL OR "
	                   "0, NULL OR 1, 0 OR NULL, 1 OR NULL, NULL OR NULL");

	CHECK_COLUMN(result, 0, {0});
	CHECK_COLUMN(result, 1, {1});
	CHECK_COLUMN(result, 2, {1});
	CHECK_COLUMN(result, 3, {1});
	CHECK_COLUMN(result, 4, {Value()});
	CHECK_COLUMN(result, 5, {1});
	CHECK_COLUMN(result, 6, {Value()});
	CHECK_COLUMN(result, 7, {1});
	CHECK_COLUMN(result, 8, {Value()});

	// NOT
	result = con.Query("SELECT NOT(0), NOT(1), NOT(NULL)");
	CHECK_COLUMN(result, 0, {1});
	CHECK_COLUMN(result, 1, {0});
	CHECK_COLUMN(result, 2, {Value()});

	// IS NULL
	result = con.Query(
	    "SELECT NULL IS NULL, NULL IS NOT NULL, 42 IS NULL, 42 IS NOT NULL");
	CHECK_COLUMN(result, 0, {1});
	CHECK_COLUMN(result, 1, {0});
	CHECK_COLUMN(result, 2, {0});
	CHECK_COLUMN(result, 3, {1});

	// Comparisions
	result =
	    con.Query("SELECT NULL = NULL, NULL <> NULL, 42 = NULL, 42 <> NULL");
	CHECK_COLUMN(result, 0, {Value()});
	CHECK_COLUMN(result, 1, {Value()});
	CHECK_COLUMN(result, 2, {Value()});
	CHECK_COLUMN(result, 3, {Value()});
}

// NOT
