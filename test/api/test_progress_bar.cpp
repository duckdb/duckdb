#include <duckdb/execution/executor.hpp>
#include <future>
#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/progress_bar.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Progress Bar", "[api]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.context->test = true;

	REQUIRE_NO_FAIL(con.Query("create  table tbl as select range a, mod(range,10) b from range(10000000);"));
	REQUIRE_NO_FAIL(con.Query("create  table tbl_2 as select range a from range(10000000);"));

	REQUIRE_NO_FAIL(con.Query("PRAGMA set_progress_bar_time=10"));
	//! Simple Aggregation
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl"));
	REQUIRE(con.context->progress_bar->IsPercentageValid());

	//! Simple Join
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl inner join tbl_2 on (tbl.a = tbl_2.a)"));
	REQUIRE(con.context->progress_bar->IsPercentageValid());

	//! Subquery
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl where a = (select min(a) from tbl_2)"));
	REQUIRE(con.context->progress_bar->IsPercentageValid());

	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl where a = (select min(b) from tbl)"));
	REQUIRE(con.context->progress_bar->IsPercentageValid());

	//! Test Multiple threads
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=4"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA force_parallelism"));

	//! Simple Aggregation
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl"));
	REQUIRE(con.context->progress_bar->IsPercentageValid());

	//! Simple Join
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl inner join tbl_2 on (tbl.a = tbl_2.a)"));
	REQUIRE(con.context->progress_bar->IsPercentageValid());

	//! Subquery
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl where a = (select min(a) from tbl_2)"));
	REQUIRE(con.context->progress_bar->IsPercentageValid());

	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl where a = (select min(b) from tbl)"));
	REQUIRE(con.context->progress_bar->IsPercentageValid());
}