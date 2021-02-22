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
	ProgressBar progress_check(&con.context->executor, 0, 10, true);
	REQUIRE_NO_FAIL(con.Query("create  table tbl as select range a from range(100000000);"));
	REQUIRE_NO_FAIL(con.Query("create  table tbl_2 as select range a from range(1000);"));

	//! Simple Aggregation
	progress_check.Start();
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl"));
	progress_check.Stop();
	REQUIRE(progress_check.IsPercentageValid());

	//! Simple Join
	progress_check.Start();
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl inner join tbl_2 on (tbl.a = tbl_2.a)"));
	progress_check.Stop();
	REQUIRE(progress_check.IsPercentageValid());


	//! Test Multiple threads
	REQUIRE_NO_FAIL(con.Query("PRAGMA threads=4"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA force_parallelism"));

	//! Simple Aggregation
	progress_check.Start();
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl"));
	progress_check.Stop();
	REQUIRE(progress_check.IsPercentageValid());

	//! Simple Join
	progress_check.Start();
	REQUIRE_NO_FAIL(con.Query("select count(*) from tbl inner join tbl_2 on (tbl.a = tbl_2.a)"));
	progress_check.Stop();
	REQUIRE(progress_check.IsPercentageValid());
}

TEST_CASE("Test Progress Bar CSV Reader", "[api]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	ProgressBar progress_check(&con.context->executor, 0, 1, false);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER, c VARCHAR(10));"));
	//! Simple Aggregation
	progress_check.Start();
	REQUIRE_NO_FAIL(con.Query("COPY test FROM 'test/sql/copy/csv/data/test/test.csv';"));
	progress_check.Stop();
	REQUIRE(progress_check.IsPercentageValid());
}