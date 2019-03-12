#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test arithmetic statements", "[arithmetic]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3), (NULL)"));

	// comparisons involving arithmetic
	// these are interesting because these will be folded by optimizers
	// so we test if the optimizers work correctly
	// addition is unordered (i.e. i+2=2+i)
	// i+2=5 => i=3
	result = con.Query("SELECT i+2=5, 5=i+2 FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));
	// 2+i=5 => i=3
	result = con.Query("SELECT 2+i=5, 5=2+i FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));
	// multiplication is unordered
	// i*2=6 => i=3
	result = con.Query("SELECT i*2=6, 6=i*2 FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));
	// 2*i=6 => i=3
	result = con.Query("SELECT 2*i=6, 6=2*i FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));
	// i*2=5 (this comparison is always FALSE, except if i is NULL in which case it is NULL)
	result = con.Query("SELECT i*2=5 FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, false}));
	// i*0=5
	result = con.Query("SELECT i*0=5 FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, false}));
	// subtraction is ordered
	// i-2=1 => i=3
	result = con.Query("SELECT i-2=1, 1=i-2 FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, false, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, false, true}));
	// 3-i=1 => i=2
	result = con.Query("SELECT 3-i=1, 1=3-i FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, true, false}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, false}));
	// non-equality comparisons should also be flipped in this case
	// 3-i<2 => i>2
	result = con.Query("SELECT 3-i<2, 2>3-i FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, true, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, true}));
	result = con.Query("SELECT 3-i<=1, 1>=3-i FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, true, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, true}));
	// division is ordered
	// i/2=1 => i>=2 or i<=3
	result = con.Query("SELECT i/2=1, 1=i/2 FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, true, true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, true}));
	// 2/i=1 => i=2
	result = con.Query("SELECT 2/i=1, 1=2/i FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, true, false}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, false}));
	// 3/i=2 => i=2
	result = con.Query("SELECT 2/i=1, 1=2/i FROM integers ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), false, true, false}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), false, true, false}));

}
