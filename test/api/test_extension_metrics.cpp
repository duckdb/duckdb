#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/profiler/metrics_manager.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/main/settings.hpp"

#include <thread>

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
	Settings::Set<ProfilingOutputSetting>(*con.context, SetScope::SESSION, Value(TestCreatePath(profile_name)));
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

// Adds one millisecond to a timing metric for every chunk, from every thread that runs the scan.
static atomic<idx_t> add_timing_calls {0};
static void AddTimingExec(DataChunk &args, ExpressionState &state, Vector &result) {
	QueryProfiler::Get(state.GetContext()).AddToMetricTiming("test.time_added", 1000000);
	add_timing_calls++;
	result.Reference(args.data[0]);
}

TEST_CASE("Time measured elsewhere can be added to a timing metric", "[api][intraquery]") {
	if (STANDARD_VECTOR_SIZE < 1024) {
		// hundreds of thousands of chunks would take too long
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	ScalarFunction fn("add_timing", {LogicalType::BIGINT}, LogicalType::BIGINT, AddTimingExec);
	fn.SetStability(FunctionStability::VOLATILE);
	RegisterFunction(con, std::move(fn));
	SetUpParallelScan(con, "test_metric_timing_output.txt");

	add_timing_calls = 0;
	REQUIRE_NO_FAIL(con.Query("SELECT max(add_timing(i)) FROM numbers"));
	// one millisecond for each call, reported in seconds
	REQUIRE(add_timing_calls > 1);
	auto seconds = GetProfiledMetric(con, "test.time_added");
	REQUIRE(!seconds.IsNull());
	REQUIRE(seconds.GetValue<double>() == Approx(static_cast<double>(add_timing_calls.load()) / 1000.0));
}

// While the query binds, before its plan is built: sets one metric if it is tracked, one whether or not it is, one that
// was not registered, and two of the profiler's own, one of which it may leave out.
static bool tracked_while_binding = false;
static unique_ptr<FunctionData> SetMetricBind(BindScalarFunctionInput &input) {
	auto &profiler = QueryProfiler::Get(input.GetClientContext());
	tracked_while_binding = profiler.MetricIsTracked("test.set_if_tracked");
	if (tracked_while_binding) {
		profiler.SetMetric("test.set_if_tracked", Value::UBIGINT(42));
	}
	profiler.SetMetric("test.set_always", Value::UBIGINT(7));
	profiler.SetMetric("test.not_registered", Value::UBIGINT(1));
	profiler.SetMetric("query.total_rows_scanned", Value::UBIGINT(123456789));
	profiler.SetMetric("query.total_bytes_scanned", Value::UBIGINT(123456789));
	return nullptr;
}

static void PassThroughExec(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(args.data[0]);
}

TEST_CASE("A metric can be set while the query binds", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	ScalarFunction fn("set_metric_while_binding", {LogicalType::BIGINT}, LogicalType::BIGINT, PassThroughExec);
	fn.SetBindCallback(SetMetricBind);
	RegisterFunction(con, std::move(fn));
	auto &metrics_manager = MetricsManager::Get(*con.context);
	metrics_manager.RegisterMetric({"test.set_if_tracked", "uint64", "Set while binding if tracked", "values"});
	metrics_manager.RegisterMetric({"test.set_always", "uint64", "Set while binding", "values"});
	// the profiler's own metrics cannot be registered
	metrics_manager.RegisterMetric({"query.total_bytes_scanned", "uint64", "Not an extension's", "bytes"});
	REQUIRE(!metrics_manager.IsRegisteredMetric("query.total_bytes_scanned"));
	metrics_manager.RegisterMetric({"optimizer.window_join_pushdown", "double", "Not an extension's", "seconds"});
	REQUIRE(!metrics_manager.IsRegisteredMetric("optimizer.window_join_pushdown"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers AS SELECT range AS i FROM range(10)"));
	con.EnableProfiling();
	Settings::Set<ProfilingOutputSetting>(*con.context, SetScope::SESSION,
	                                      Value(TestCreatePath("test_set_metric_output.txt")));

	REQUIRE_NO_FAIL(con.Query("SELECT max(set_metric_while_binding(i)) FROM numbers"));
	REQUIRE(tracked_while_binding);
	REQUIRE(GetProfiledMetric(con, "test.set_if_tracked") == Value::UBIGINT(42));
	REQUIRE(GetProfiledMetric(con, "test.set_always") == Value::UBIGINT(7));
	REQUIRE(GetProfiledMetric(con, "test.not_registered").IsNull());
	// the profiler's own metrics keep its values, and stay out when it leaves them out
	auto rows_scanned = GetProfiledMetric(con, "query.total_rows_scanned");
	REQUIRE(!rows_scanned.IsNull());
	REQUIRE(rows_scanned.GetValue<idx_t>() == 10);
	// a scan of a table does not count the bytes it scans, so the profiler leaves this out
	REQUIRE(GetProfiledMetric(con, "query.total_bytes_scanned").IsNull());

	// neither is reported when it is not tracked
	REQUIRE_NO_FAIL(con.Query("SET tracked_metrics = ['query.*']"));
	REQUIRE_NO_FAIL(con.Query("SELECT max(set_metric_while_binding(i)) FROM numbers"));
	REQUIRE(!tracked_while_binding);
	REQUIRE(GetProfiledMetric(con, "test.set_if_tracked").IsNull());
	REQUIRE(GetProfiledMetric(con, "test.set_always").IsNull());
}

// Updates the profiler's own metrics by name, as an extension may, from every thread that runs the scan.
static atomic<bool> added_join_order_time {false};
static void UpdateOwnMetricsExec(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &profiler = QueryProfiler::Get(state.GetContext());
	profiler.AddToMetricCounter("storage.wal_replay_entry_count", args.size());
	if (!added_join_order_time.exchange(true)) {
		profiler.AddToMetricTiming("optimizer.join_order", 1000000000);
	}
	result.Reference(args.data[0]);
}

TEST_CASE("Updates by name to the profiler's own metrics land with its own updates", "[api][intraquery]") {
	DuckDB db(nullptr);
	Connection con(db);
	ScalarFunction fn("update_own_metrics", {LogicalType::BIGINT}, LogicalType::BIGINT, UpdateOwnMetricsExec);
	fn.SetStability(FunctionStability::VOLATILE);
	RegisterFunction(con, std::move(fn));
	SetUpParallelScan(con, "test_own_metrics_by_name_output.txt");

	added_join_order_time = false;
	REQUIRE_NO_FAIL(con.Query("SELECT max(update_own_metrics(i)) FROM numbers"));
	REQUIRE(GetProfiledMetric(con, "storage.wal_replay_entry_count") == Value::UBIGINT(ROW_COUNT));
	// the optimizer timed join_order itself, and the function added a second to it
	REQUIRE(GetProfiledMetric(con, "optimizer.join_order").GetValue<double>() >= 1.0);
	auto &query_metrics = QueryProfiler::Get(*con.context).GetQueryMetrics();
	REQUIRE(query_metrics.GetStringCounter("storage.wal_replay_entry_count") == ROW_COUNT);
	REQUIRE(query_metrics.GetMetricCounters()["storage.wal_replay_entry_count"] == ROW_COUNT);
	REQUIRE(query_metrics.GetStringMetricInSeconds("optimizer.join_order") >= 1.0);

	// a query that does not update them counts from zero
	REQUIRE_NO_FAIL(con.Query("SELECT max(i) FROM numbers"));
	REQUIRE(query_metrics.GetStringCounter("storage.wal_replay_entry_count") == 0);
	REQUIRE(query_metrics.GetStringMetricInSeconds("optimizer.join_order") < 1.0);
}

TEST_CASE("Registering a metric returns its index, the same in every database", "[api]") {
	DuckDB db(nullptr);
	DuckDB other_db(nullptr);
	auto &metrics_manager = MetricsManager::Get(*db.instance);
	auto &other_manager = MetricsManager::Get(*other_db.instance);
	auto count = metrics_manager.GetMetricCount();
	auto first = metrics_manager.RegisterMetric({"test.first", "uint64", "First", "rows"});
	auto second = metrics_manager.RegisterMetric({"test.second", "double", "Second", "seconds"});
	REQUIRE(first != second);
	REQUIRE(first >= MetricsManager::InternalMetricCount());
	REQUIRE(metrics_manager.GetMetricCount() == count + 2);
	REQUIRE(MetricsManager::GetMetric(first).name == "test.first");
	REQUIRE(MetricsManager::GetMetric(second).name == "test.second");
	// registering a name again returns the index it already has, and its type must stay the same
	REQUIRE(metrics_manager.RegisterMetric({"test.first", "uint64", "First", "rows"}) == first);
	REQUIRE_THROWS_WITH(metrics_manager.RegisterMetric({"test.first", "double", "First", "seconds"}),
	                    Catch::Contains("already registered"));
	// another database gets the same index, and lists the metric once it registered it too
	REQUIRE(!other_manager.IsRegisteredMetric("test.second"));
	REQUIRE(other_manager.RegisterMetric({"test.second", "double", "Second", "seconds"}) == second);
	REQUIRE(other_manager.IsRegisteredMetric("test.second"));
	REQUIRE(!other_manager.IsRegisteredMetric("test.first"));
	REQUIRE_THROWS_WITH(other_manager.RegisterMetric({"test.first", "double", "First", "seconds"}),
	                    Catch::Contains("already registered"));
	// the profiler's own metrics are not registered again; they return their own index
	REQUIRE(metrics_manager.RegisterMetric({"optimizer.join_order", "double", "Not an extension's", "seconds"}) ==
	        MetricsManager::OptimizerMetricIndex(OptimizerType::JOIN_ORDER));
	REQUIRE(metrics_manager.RegisterMetric({"storage.wal_replay_entry_count", "uint64", "Not an extension's",
	                                        "entries"}) == MetricStorageWALReplayEntryCount::MetricIndex);
	REQUIRE_THROWS_WITH(metrics_manager.RegisterMetric({"optimizer.join_order", "uint64", "Not a timing", "rows"}),
	                    Catch::Contains("one of the profiler's own"));
	REQUIRE(metrics_manager.GetMetricCount() == count + 2);
	REQUIRE(!metrics_manager.IsRegisteredMetric("optimizer.join_order"));
}

// The indexes of the metrics the next test registers, for the scalar function to update.
static idx_t rows_index;
static idx_t time_index;
static idx_t timer_index;
static atomic<idx_t> indexed_calls {0};

// Updates registered metrics by index for every chunk, from every thread that runs the scan.
static void UpdateByIndexExec(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &profiler = QueryProfiler::Get(state.GetContext());
	profiler.AddToMetricCounter(rows_index, args.size());
	profiler.AddToMetricTiming(time_index, 1000000);
	{ auto timer = profiler.StartTimer(timer_index); }
	indexed_calls++;
	result.Reference(args.data[0]);
}

TEST_CASE("Registered metrics can be updated by index from several threads at once", "[api][intraquery]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &metrics_manager = MetricsManager::Get(*con.context);
	rows_index = metrics_manager.RegisterMetric({"test.indexed_rows", "uint64", "Rows counted by index", "rows"});
	time_index = metrics_manager.RegisterMetric({"test.indexed_time", "double", "Time added by index", "seconds"});
	timer_index = metrics_manager.RegisterMetric({"test.indexed_timer", "double", "Time timed by index", "seconds"});
	ScalarFunction fn("update_by_index", {LogicalType::BIGINT}, LogicalType::BIGINT, UpdateByIndexExec);
	fn.SetStability(FunctionStability::VOLATILE);
	RegisterFunction(con, std::move(fn));
	SetUpParallelScan(con, "test_indexed_metrics_output.txt");

	indexed_calls = 0;
	REQUIRE_NO_FAIL(con.Query("SELECT max(update_by_index(i)) FROM numbers"));
	REQUIRE(indexed_calls > 1);
	REQUIRE(GetProfiledMetric(con, "test.indexed_rows") == Value::UBIGINT(ROW_COUNT));
	REQUIRE(GetProfiledMetric(con, "test.indexed_time").GetValue<double>() ==
	        Approx(static_cast<double>(indexed_calls.load()) / 1000.0));
	REQUIRE(!GetProfiledMetric(con, "test.indexed_timer").IsNull());
	auto &query_metrics = QueryProfiler::Get(*con.context).GetQueryMetrics();
	REQUIRE(query_metrics.GetMetricTimings()["test.indexed_timer"].has_bounds);

	// a query that does not update them does not report them
	REQUIRE_NO_FAIL(con.Query("SELECT max(i) FROM numbers"));
	REQUIRE(GetProfiledMetric(con, "test.indexed_rows").IsNull());
}

// Updates a registered metric both by name and by index.
static void UpdateBothWaysExec(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &profiler = QueryProfiler::Get(state.GetContext());
	profiler.AddToMetricCounter(rows_index, args.size());
	profiler.AddToMetricCounter("test.both_ways", args.size());
	result.Reference(args.data[0]);
}

TEST_CASE("A metric updated both by name and by index adds up", "[api][intraquery]") {
	DuckDB db(nullptr);
	Connection con(db);
	rows_index = MetricsManager::Get(*con.context).RegisterMetric({"test.both_ways", "uint64", "Both ways", "rows"});
	ScalarFunction fn("update_both_ways", {LogicalType::BIGINT}, LogicalType::BIGINT, UpdateBothWaysExec);
	fn.SetStability(FunctionStability::VOLATILE);
	RegisterFunction(con, std::move(fn));
	SetUpParallelScan(con, "test_both_ways_output.txt");

	REQUIRE_NO_FAIL(con.Query("SELECT max(update_both_ways(i)) FROM numbers"));
	REQUIRE(GetProfiledMetric(con, "test.both_ways") == Value::UBIGINT(2 * ROW_COUNT));
	auto &query_metrics = QueryProfiler::Get(*con.context).GetQueryMetrics();
	REQUIRE(query_metrics.GetStringCounter("test.both_ways") == 2 * ROW_COUNT);
	REQUIRE(query_metrics.GetMetricCounters()["test.both_ways"] == 2 * ROW_COUNT);
}

// Registers a metric the first time it runs, while the query is running, and counts by its index.
static atomic<bool> registered_while_running {false};
static idx_t late_index;
static void RegisterWhileRunningExec(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetClientContext();
	if (!registered_while_running.exchange(true)) {
		late_index = MetricsManager::Get(context).RegisterMetric(
		    {"test.registered_while_running", "uint64", "Registered while a query ran", "rows"});
	}
	QueryProfiler::Get(context).AddToMetricCounter(late_index, args.size());
	result.Reference(args.data[0]);
}

TEST_CASE("A metric registered while a query runs is reported for that query", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	ScalarFunction fn("register_while_running", {LogicalType::BIGINT}, LogicalType::BIGINT, RegisterWhileRunningExec);
	fn.SetStability(FunctionStability::VOLATILE);
	RegisterFunction(con, std::move(fn));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers AS SELECT range AS i FROM range(100)"));
	REQUIRE_NO_FAIL(con.Query("SET threads = 1"));
	con.EnableProfiling();
	Settings::Set<ProfilingOutputSetting>(*con.context, SetScope::SESSION,
	                                      Value(TestCreatePath("test_late_registration_output.txt")));

	registered_while_running = false;
	REQUIRE_NO_FAIL(con.Query("SELECT max(register_while_running(i)) FROM numbers"));
	REQUIRE(GetProfiledMetric(con, "test.registered_while_running") == Value::UBIGINT(100));
}

TEST_CASE("Updates by index may come from another thread while queries start and metrics are registered", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &metrics_manager = MetricsManager::Get(*con.context);
	auto counter = metrics_manager.RegisterMetric({"test.background_counter", "uint64", "Counted aside", "rows"});
	REQUIRE_NO_FAIL(con.Query("SET threads = 1"));
	con.EnableProfiling();
	Settings::Set<ProfilingOutputSetting>(*con.context, SetScope::SESSION,
	                                      Value(TestCreatePath("test_background_updates_output.txt")));

	// like an extension's background thread, which reports to the profiler of the connection that started it
	auto &profiler = QueryProfiler::Get(*con.context);
	atomic<bool> done {false};
	atomic<idx_t> latest {counter};
	std::thread updater([&]() {
		while (!done) {
			profiler.AddToMetricCounter(counter, 1);
			profiler.AddToMetricCounter(latest.load(), 1);
		}
	});
	// enough to cross into another chunk of slots
	for (idx_t i = 0; i < 70; i++) {
		// the newest metric has no slot yet, so the updates make the connection allocate more while queries start
		latest = metrics_manager.RegisterMetric({"test.background_" + to_string(i), "uint64", "Aside", "rows"});
		REQUIRE_NO_FAIL(con.Query("SELECT 42"));
	}
	done = true;
	updater.join();
}

// Updates by an index the process does not know, and by indexes of the wrong type.
static idx_t ratio_index;
static void UpdateBadIndexesExec(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &profiler = QueryProfiler::Get(state.GetContext());
	auto unknown_index = MetricsManager::KnownMetricCount() + 1000;
	profiler.AddToMetricCounter(unknown_index, args.size());
	profiler.AddToMetricTiming(unknown_index, 1000);
	{ auto timer = profiler.StartTimer(unknown_index); }
	// a counter of a "double", and a timing of a "uint64"
	profiler.AddToMetricCounter(ratio_index, args.size());
	profiler.AddToMetricTiming(rows_index, 1000);
	profiler.AddToMetricCounter(rows_index, args.size());
	result.Reference(args.data[0]);
}

TEST_CASE("Updates by an index that does not fit are left out", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &metrics_manager = MetricsManager::Get(*con.context);
	rows_index = metrics_manager.RegisterMetric({"test.fit_rows", "uint64", "Rows", "rows"});
	ratio_index = metrics_manager.RegisterMetric({"test.fit_ratio", "double", "Not a counter", "ratio"});
	ScalarFunction fn("update_bad_indexes", {LogicalType::BIGINT}, LogicalType::BIGINT, UpdateBadIndexesExec);
	fn.SetStability(FunctionStability::VOLATILE);
	RegisterFunction(con, std::move(fn));
	SetUpParallelScan(con, "test_bad_indexes_output.txt");

	REQUIRE_NO_FAIL(con.Query("SELECT max(update_bad_indexes(i)) FROM numbers"));
	REQUIRE(GetProfiledMetric(con, "test.fit_rows") == Value::UBIGINT(ROW_COUNT));
	REQUIRE(GetProfiledMetric(con, "test.fit_ratio").IsNull());
	// and the connection keeps working
	REQUIRE_NO_FAIL(con.Query("SELECT 42"));
	REQUIRE_NO_FAIL(con.Query("SELECT max(update_bad_indexes(i)) FROM numbers"));
}

// Counts by an index kept in a static, as an extension loaded into several databases would.
static idx_t shared_index;
static void CountBySharedIndexExec(DataChunk &args, ExpressionState &state, Vector &result) {
	QueryProfiler::Get(state.GetContext()).AddToMetricCounter(shared_index, args.size());
	result.Reference(args.data[0]);
}

TEST_CASE("An index kept in a static counts the same metric in every database", "[api]") {
	DuckDB first_db(nullptr);
	DuckDB second_db(nullptr);
	// the second database knows another metric first, so a per-database index would differ
	MetricsManager::Get(*second_db.instance).RegisterMetric({"test.other_first", "uint64", "Other", "rows"});
	shared_index = MetricsManager::Get(*first_db.instance).RegisterMetric({"test.shared", "uint64", "Shared", "rows"});
	REQUIRE(MetricsManager::Get(*second_db.instance).RegisterMetric({"test.shared", "uint64", "Shared", "rows"}) ==
	        shared_index);
	for (auto db : {&first_db, &second_db}) {
		Connection con(*db);
		ScalarFunction fn("count_by_shared_index", {LogicalType::BIGINT}, LogicalType::BIGINT, CountBySharedIndexExec);
		fn.SetStability(FunctionStability::VOLATILE);
		RegisterFunction(con, std::move(fn));
		SetUpParallelScan(con, "test_shared_index_output.txt");
		REQUIRE_NO_FAIL(con.Query("SELECT max(count_by_shared_index(i)) FROM numbers"));
		REQUIRE(GetProfiledMetric(con, "test.shared") == Value::UBIGINT(ROW_COUNT));
		REQUIRE(GetProfiledMetric(con, "test.other_first").IsNull());
	}
}
