#include "catch.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/profiler/metrics.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

static idx_t LiveMetric(Connection &con, const char *name) {
	auto metrics = con.context->GetLiveQueryMetrics();
	auto entry = metrics.find(name);
	REQUIRE(entry != metrics.end());
	return entry->second.GetValue<idx_t>();
}

TEST_CASE("Test live query metrics are readable while a query runs", "[api]") {
	auto path = TestCreatePath("live_query_metrics.db");
	DeleteDatabase(path);

	// persist a table to disk, then reopen the database so the buffer cache is cold
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range AS i FROM range(10000000)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);

		// pause a streaming scan after its first chunk: it has read from storage but is still running
		auto streaming = OpenStream(con, "SELECT i FROM t");
		REQUIRE(streaming->Fetch());
		REQUIRE(LiveMetric(con, MetricIOTotalBytesRead::Name) > 0);

		// the live snapshot uses the same names as the final profile
		auto metrics = con.context->GetLiveQueryMetrics();
		REQUIRE(metrics.count(MetricSystemTotalMemoryAllocated::Name) == 1);
		REQUIRE(metrics.count(MetricIOTotalBytesWritten::Name) == 1);
	}

	DeleteDatabase(path);
}

TEST_CASE("Test live query metrics of a running query are readable from another connection", "[api]") {
	auto path = TestCreatePath("live_query_metrics_remote.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range AS i FROM range(10000000)"));
		REQUIRE_NO_FAIL(con.Query("CHECKPOINT"));
	}
	{
		DuckDB db(path);
		Connection con(db);
		Connection monitor(db);

		// pause a streaming scan after its first chunk; a second connection sees its bytes read so far
		auto streaming = OpenStream(con, "SELECT i FROM t");
		REQUIRE(streaming->Fetch());
		auto result = monitor.Query("SELECT CAST(metric_value AS UBIGINT) > 0 FROM duckdb_live_query_metrics() "
		                            "WHERE metric_name = 'io.total_bytes_read' AND connection_id = " +
		                            to_string(con.context->GetConnectionId()));
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}
	DeleteDatabase(path);
}
