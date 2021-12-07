#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Pending Query API", "[api]") {
	DuckDB db;
	Connection con(db);

	auto pending_query = con.PendingQuery("SELECT SUM(i) FROM range(1000000) tbl(i)");
	auto result = pending_query->Execute();
	CHECK_COLUMN(result, 0, {Value::BIGINT(499999500000)});
}
