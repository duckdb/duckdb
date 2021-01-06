#include "catch.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;
using namespace std;

TEST_CASE("Test using connection after explicit database shutdown", "[api]") {
	DuckDB db;
	Connection con(db);

	// // check that the connection works
	// auto result = con.Query("SELECT 42");
	// REQUIRE(CHECK_COLUMN(result, 0, {42}));
	// // destroy the database
	// db.Shutdown();
	// // using the connection after shutdown results in errors
	// REQUIRE_THROWS(con.Query("SELECT 42"));
	// REQUIRE_THROWS(con.BeginTransaction());
}
