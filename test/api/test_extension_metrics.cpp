#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

using namespace duckdb;

// The metrics an extension reports through the QueryProfiler, from the functions it runs: these tests call the
// profiler the way an extension's scalar functions would.

static void RegisterFunction(Connection &con, ScalarFunction function) {
	CreateScalarFunctionInfo info(std::move(function));
	con.context->RunFunctionInTransaction(
	    [&]() { Catalog::GetSystemCatalog(*con.context).CreateFunction(*con.context, info); });
}

// A table of a million rows, profiling enabled and eight threads: a scan of the table runs on several threads, where a
// scan of range() would not.
static constexpr idx_t ROW_COUNT = 1000000;
static void SetUpParallelScan(Connection &con, const string &profile_name) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers AS SELECT range AS i FROM range(" + to_string(ROW_COUNT) + ")"));
	REQUIRE_NO_FAIL(con.Query("SET threads = 8"));
	con.EnableProfiling();
	con.context->config.profiler_save_location = TestCreatePath(profile_name);
}

// The value of the metric `key` in the profile of the last query, or NULL when it is not there.
static Value GetProfiledMetric(Connection &con, const string &key) {
	Value result;
	QueryProfiler::Get(*con.context)
	    .GetFinalizedProfileUnderLock(
	        [&](optional_ptr<const GatheredMetrics> metrics, optional_ptr<const ProfilingNode>) {
		        REQUIRE(metrics);
		        auto entry = metrics->GetMetrics().find(key);
		        if (entry != metrics->GetMetrics().end()) {
			        result = entry->second;
		        }
	        });
	return result;
}

// Adds the rows of every chunk to a counter shared by all threads, and to one of 64 counters picked by the chunk's
// first value, so the threads also add new keys to the counters while others are adding to them.
static void CountRowsExec(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &profiler = QueryProfiler::Get(state.GetContext());
	auto first = args.data[0].GetValue(0).GetValue<int64_t>();
	profiler.AddToMetricCounter("test.rows_counted", args.size());
	profiler.AddToMetricCounter("test.rows_counted_by_chunk_" + to_string(first / STANDARD_VECTOR_SIZE % 64),
	                            args.size());
	result.Reference(args.data[0]);
}

TEST_CASE("Metric counters can be added to from several threads at once", "[api][intraquery]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// hundreds of thousands of chunks would take too long
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	ScalarFunction fn("count_rows", {LogicalType::BIGINT}, LogicalType::BIGINT, CountRowsExec);
	fn.SetStability(FunctionStability::VOLATILE);
	RegisterFunction(con, std::move(fn));
	SetUpParallelScan(con, "test_metric_counters_output.txt");

	REQUIRE_NO_FAIL(con.Query("SELECT max(count_rows(i)) FROM numbers"));
	REQUIRE(GetProfiledMetric(con, "test.rows_counted") == Value::UBIGINT(ROW_COUNT));
	idx_t total_by_chunk = 0;
	for (idx_t chunk = 0; chunk < 64; chunk++) {
		auto value = GetProfiledMetric(con, "test.rows_counted_by_chunk_" + to_string(chunk));
		total_by_chunk += value.IsNull() ? 0 : value.GetValue<idx_t>();
	}
	REQUIRE(total_by_chunk == ROW_COUNT);
}

// Ends 16 scoped timers for every chunk, for 16 of 64 timing metrics picked by the chunk's first value, from every
// thread that runs the scan, so the timers also add new keys while others are ending. Core's own timers can end on
// several threads too, e.g. the vacuum tasks of a checkpoint.
static atomic<idx_t> timed_chunks {0};
static void TimeChunkExec(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &profiler = QueryProfiler::Get(state.GetContext());
	auto first = NumericCast<idx_t>(args.data[0].GetValue(0).GetValue<int64_t>()) / STANDARD_VECTOR_SIZE;
	for (idx_t i = 0; i < 16; i++) {
		auto timer = profiler.StartTimerInternal("test.chunk_time_" + to_string((first + i) % 64));
	}
	result.Reference(args.data[0]);
	timed_chunks++;
}

TEST_CASE("Scoped timers can end on several threads at once", "[api][intraquery]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// sixteen timers for each of the hundreds of thousands of chunks would take too long
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	ScalarFunction fn("time_chunk", {LogicalType::BIGINT}, LogicalType::BIGINT, TimeChunkExec);
	fn.SetStability(FunctionStability::VOLATILE);
	RegisterFunction(con, std::move(fn));
	SetUpParallelScan(con, "test_metric_timers_output.txt");

	timed_chunks = 0;
	REQUIRE_NO_FAIL(con.Query("SELECT max(time_chunk(i)) FROM numbers"));
	REQUIRE(timed_chunks > 64);
	for (idx_t chunk = 0; chunk < 64; chunk++) {
		auto seconds = GetProfiledMetric(con, "test.chunk_time_" + to_string(chunk));
		REQUIRE(!seconds.IsNull());
		REQUIRE(seconds.GetValue<double>() > 0);
	}
}
