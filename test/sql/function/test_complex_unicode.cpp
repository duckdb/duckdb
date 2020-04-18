#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test correct behavior of various string functions under complex unicode characters", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// length with grapheme clusters
	result = con.Query("SELECT length('S̈a')");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	result = con.Query("SELECT length('🤦🏼‍♂️')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT length('🤦🏼‍♂️ L🤦🏼‍♂️R 🤦🏼‍♂️')");
	REQUIRE(CHECK_COLUMN(result, 0, {7}));

	// strlen returns size in bytes
	result = con.Query("SELECT strlen('🤦🏼‍♂️')");
	REQUIRE(CHECK_COLUMN(result, 0, {17}));
	result = con.Query("SELECT strlen('S̈a')");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));

	// reverse with grapheme clusters
	result = con.Query("SELECT REVERSE('S̈a︍')");
	REQUIRE(CHECK_COLUMN(result, 0, {"a︍S̈"}));
	result = con.Query("SELECT REVERSE('Z͑ͫ̓ͪ̂ͫ̽͏̴̙̤̞͉͚̯̞̠͍A̴̵̜̰͔ͫ͗͢')");
	REQUIRE(CHECK_COLUMN(result, 0, {"A̴̵̜̰͔ͫ͗͢Z͑ͫ̓ͪ̂ͫ̽͏̴̙̤̞͉͚̯̞̠͍"}));
	result = con.Query("SELECT REVERSE('🤦🏼‍♂️')");
	REQUIRE(CHECK_COLUMN(result, 0, {"🤦🏼‍♂️"}));
	result = con.Query("SELECT REVERSE('🤦🏼‍♂️ L🤦🏼‍♂️R 🤦🏼‍♂️')");
	REQUIRE(CHECK_COLUMN(result, 0, {"🤦🏼‍♂️ R🤦🏼‍♂️L 🤦🏼‍♂️"}));
	result = con.Query("SELECT REVERSE('MotörHead')");
	REQUIRE(CHECK_COLUMN(result, 0, {"daeHrötoM"}));

	// substring with grapheme clusters
	result = con.Query("SELECT substring('🤦🏼‍♂️🤦🏼‍♂️🤦🏼‍♂️', 1, 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {"🤦🏼‍♂️"}));
	result = con.Query("SELECT substring('S̈a︍', 2, 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {"a"}));
	result = con.Query("SELECT substring('test: 🤦🏼‍♂️hello🤦🏼‍♂️ world', 7, 7)");
	REQUIRE(CHECK_COLUMN(result, 0, {"🤦🏼‍♂️hello🤦🏼‍♂️"}));
	result = con.Query("SELECT substring('S̈a', 1, 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {"S̈"}));
}
