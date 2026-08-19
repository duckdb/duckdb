#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/arrow/physical_arrow_collector.hpp"
#include "duckdb/common/query_parameters.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/storage/storage_info.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <thread>

using namespace duckdb;

namespace {

//! A listener the callback wakes (Notify() -> cv.notify_one()); the consumer waits on it.
//! Spurious notifications are part of the contract.
struct NotifyListener {
	std::mutex lock;
	std::condition_variable cv;
	bool woken = false;

	void Notify() {
		{
			std::lock_guard<std::mutex> guard(lock);
			woken = true;
		}
		cv.notify_one();
	}
	//! Returns false on timeout
	bool WaitAndReset(std::chrono::milliseconds timeout) {
		std::unique_lock<std::mutex> guard(lock);
		if (!cv.wait_for(guard, timeout, [&]() { return woken; })) {
			return false;
		}
		woken = false;
		return true;
	}
};

QueryParameters AsyncParameters() {
	QueryParameters parameters(QueryResultOutputType::ALLOW_STREAMING);
	parameters.execution_mode = QueryResultExecutionMode::ASYNC;
	return parameters;
}

unique_ptr<StreamQueryResult> ExecuteAsync(Connection &con, const string &query) {
	auto pending = con.PendingQuery(query, AsyncParameters());
	if (pending->HasError()) {
		FAIL(pending->GetError());
	}
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	return unique_ptr_cast<QueryResult, StreamQueryResult>(std::move(result));
}

bool DeadlinePassed(const std::chrono::steady_clock::time_point &deadline) {
	return std::chrono::steady_clock::now() >= deadline;
}

std::chrono::steady_clock::time_point MakeDeadline() {
	// Bounds every drain loop so a liveness bug fails the test instead of hanging the suite
	return std::chrono::steady_clock::now() + std::chrono::seconds(60);
}

//! Install the connection-level default notifier that wakes the listener
void SetDefaultNotifier(Connection &con, NotifyListener &listener) {
	auto &config = ClientConfig::GetConfig(*con.context);
	config.default_notify_callback = [&listener]() {
		listener.Notify();
	};
}

//! Render the physical plan text so tests can assert the plan shape
string PhysicalPlanText(Connection &con, const string &query) {
	auto explain_result = con.Query("EXPLAIN " + query);
	if (explain_result->HasError()) {
		FAIL(explain_result->GetError());
	}
	string plan;
	for (idx_t row = 0; row < explain_result->RowCount(); row++) {
		plan += explain_result->GetValue(1, row).ToString();
	}
	return plan;
}

//! Verify a chunk column continues the ascending sequence; returns the next expected value.
//! Reads the vector data directly: the consumer must stay fast enough to outrun the
//! producers, or the drain loops never park and the wake edges go untested.
int64_t VerifyAscending(DataChunk &chunk, int64_t next_value, idx_t column = 0) {
	chunk.Flatten();
	if (!FlatVector::Validity(chunk.data[column]).CheckAllValid(chunk.size())) {
		FAIL("Unexpected NULL in ascending sequence");
	}
	auto data = FlatVector::GetData<int64_t>(chunk.data[column]);
	for (idx_t i = 0; i < chunk.size(); i++) {
		if (data[i] != next_value) {
			FAIL(StringUtil::Format("Out-of-order row: expected %lld, got %lld", next_value, data[i]));
		}
		next_value++;
	}
	return next_value;
}

//! Listener that also records callbacks running on the consumer's own stack inside an engine call
struct StackCheckListener {
	NotifyListener listener;
	std::atomic<bool> in_call {false};
	std::atomic<idx_t> consumer_stack_notifications {0};
	std::thread::id consumer_thread;
};

} // namespace

#ifndef DUCKDB_NO_THREADS

TEST_CASE("Async stream result completes using TryFetch only", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// A tiny buffer forces many block/restart cycles on the producer sinks
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM range(500000) t(i)");
	auto deadline = MakeDeadline();
	idx_t row_count = 0;
	int64_t sum = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				sum += chunk->GetValue(0, i).GetValue<int64_t>();
			}
			row_count += chunk->size();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE((execution_result == StreamExecutionResult::BLOCKED ||
		         execution_result == StreamExecutionResult::CHUNK_NOT_READY));
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(row_count == 500000);
	REQUIRE(sum == 124999750000);

	// The connection is usable again after the stream is drained
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Async stream result wakes a parked consumer", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	// The callback must never run on the consumer's stack inside an engine call
	StackCheckListener state;
	state.consumer_thread = std::this_thread::get_id();
	auto &config = ClientConfig::GetConfig(*con.context);
	config.default_notify_callback = [&state]() {
		if (std::this_thread::get_id() == state.consumer_thread && state.in_call.load()) {
			state.consumer_stack_notifications++;
		}
		state.listener.Notify();
	};

	state.in_call = true;
	auto stream = ExecuteAsync(con, "SELECT i FROM range(500000) t(i)");
	state.in_call = false;

	auto deadline = MakeDeadline();
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		state.in_call = true;
		auto execution_result = stream->TryFetchChunk(chunk);
		state.in_call = false;
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			row_count += chunk->size();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE((execution_result == StreamExecutionResult::BLOCKED ||
		         execution_result == StreamExecutionResult::CHUNK_NOT_READY));
		// Park until the engine wakes us. A missed notification fails here by timeout.
		REQUIRE(state.listener.WaitAndReset(std::chrono::seconds(10)));
	}
	REQUIRE(row_count == 500000);
	REQUIRE(state.consumer_stack_notifications.load() == 0);
}

TEST_CASE("Async stream result wakes on chunks with rows but no data bytes", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Empty-struct chunks have rows but zero data bytes: readiness and the notify edge
	// must both follow the chunk queue, not the byte count
	NotifyListener listener;
	SetDefaultNotifier(con, listener);
	auto stream = ExecuteAsync(con, "SELECT struct_pack() FROM range(100000) t(i)");

	auto deadline = MakeDeadline();
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			row_count += chunk->size();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	REQUIRE(row_count == 100000);
}

TEST_CASE("Async stream result survives tiny stream buffers", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// streaming_buffer_size has no lower bound; an async buffer must still admit chunks
	for (auto size : {"'0B'", "'1B'"}) {
		REQUIRE_NO_FAIL(con.Query(string("SET streaming_buffer_size=") + size));
		auto stream = ExecuteAsync(con, "SELECT i FROM range(10000) t(i)");
		auto deadline = MakeDeadline();
		idx_t row_count = 0;
		while (true) {
			REQUIRE(!DeadlinePassed(deadline));
			unique_ptr<DataChunk> chunk;
			auto execution_result = stream->TryFetchChunk(chunk);
			if (execution_result == StreamExecutionResult::CHUNK_READY) {
				row_count += chunk->size();
				continue;
			}
			if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
				break;
			}
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		}
		REQUIRE(row_count == 10000);

		// The buffer floor also fixes the sync path, which used to end silently with zero rows at 0B
		auto sync_stream = con.SendQuery("SELECT i FROM range(10000) t(i)", QueryParameters(true));
		REQUIRE(!sync_stream->HasError());
		idx_t sync_rows = 0;
		while (auto sync_chunk = sync_stream->Fetch()) {
			sync_rows += sync_chunk->size();
		}
		REQUIRE(sync_rows == 10000);
	}
}

TEST_CASE("Async execution mode applies to the final statement of a multi-statement query", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Intermediates are materialized and sync (the connection default is ignored for them);
	// only the final result is async and notifies
	NotifyListener listener;
	SetDefaultNotifier(con, listener);
	auto result = con.SendQuery("SELECT 1; SELECT 2", AsyncParameters());
	REQUIRE(!result->HasError());
	// Walk to the final result of the chain
	QueryResult *last = result.get();
	while (last->next) {
		last = last->next.get();
		REQUIRE(!last->HasError());
	}
	REQUIRE(last->GetResultType() == QueryResultType::STREAM_RESULT);
	auto &stream = last->Cast<StreamQueryResult>();
	auto deadline = MakeDeadline();
	idx_t row_count = 0;
	int64_t value = -1;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream.TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			row_count += chunk->size();
			value = chunk->GetValue(0, 0).GetValue<int64_t>();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(row_count == 1);
	REQUIRE(value == 2);
}

TEST_CASE("Async stream result wakes the consumer on the terminal transition", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Produces no rows, so the only notification can come from execution finishing
	NotifyListener listener;
	SetDefaultNotifier(con, listener);
	auto stream = ExecuteAsync(con, "SELECT i FROM range(50000000) t(i) WHERE i < 0");

	auto deadline = MakeDeadline();
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_ERROR);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			// TryFetchChunk only reports CHUNK_READY for a non-empty chunk
			FAIL("a rowless query produced a chunk");
		}
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
}

TEST_CASE("Async stream result wakes a parked consumer on interrupt", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	NotifyListener listener;
	SetDefaultNotifier(con, listener);
	auto stream = ExecuteAsync(con, "SELECT i FROM range(100000000) t(i)");

	// Deliberately parks without fetching, to pin the one case where only the interrupt
	// notification can wake us: producers parked on a full buffer. The loop tolerates the
	// few early append notifications.
	std::thread interruptor([&con]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		con.Interrupt();
	});
	auto deadline = MakeDeadline();
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		if (!listener.WaitAndReset(std::chrono::seconds(10))) {
			FAIL("the interrupt did not wake the parked consumer");
		}
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::EXECUTION_ERROR) {
			break;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_FINISHED);
	}
	interruptor.join();
	REQUIRE(stream->HasError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "INTERRUPT"));
}

TEST_CASE("Async stream result still supports blocking materialization", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM range(500000) t(i)");
	auto materialized = stream->Materialize();
	REQUIRE(!materialized->HasError());
	REQUIRE(materialized->RowCount() == 500000);
}

TEST_CASE("Sync queries ignore the connection default notifier", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	NotifyListener listener;
	SetDefaultNotifier(con, listener);

	// Materialized and sync streaming queries run normally with a default installed
	auto materialized = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(materialized, 0, {42}));
	auto sync_stream = con.SendQuery("SELECT i FROM range(100) t(i)", QueryParameters(true));
	REQUIRE(!sync_stream->HasError());
	idx_t sync_rows = 0;
	while (auto chunk = sync_stream->Fetch()) {
		sync_rows += chunk->size();
	}
	REQUIRE(sync_rows == 100);

	// The same default still drives a later async query on this connection
	auto stream = ExecuteAsync(con, "SELECT i FROM range(1000) t(i)");
	auto deadline = MakeDeadline();
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			row_count += chunk->size();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	REQUIRE(row_count == 1000);
}

TEST_CASE("The connection default notifier observes completion without fetching", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	NotifyListener listener;
	SetDefaultNotifier(con, listener);

	auto pending = con.PendingQuery("SELECT 42", AsyncParameters());
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	// The notifier was armed before execution started, so the first chunk or the terminal
	// transition must notify even though nothing has been fetched yet
	REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));

	auto &stream = result->Cast<StreamQueryResult>();
	auto deadline = MakeDeadline();
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream.TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			row_count += chunk->size();
			REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 42);
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(row_count == 1);
}

TEST_CASE("Async mode is unaffected by an installed custom result collector", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Canary for the invariant async mode relies on: the get_result_collector hook only ever
	// applies to materialized results, so a custom collector can never observe ASYNC
	auto &config = ClientConfig::GetConfig(*con.context);
	config.get_result_collector = [](ClientContext &context, PreparedStatementData &data) {
		return PhysicalArrowCollector::Create(context, data, 100);
	};

	// Sanity: the hook is live on the materialized path. Via PendingQuery: Connection::Query
	// asserts a materialized result and does not support custom collectors.
	auto materialized_pending =
	    con.PendingQuery("SELECT 42", QueryParameters(QueryResultOutputType::FORCE_MATERIALIZED));
	REQUIRE(!materialized_pending->HasError());
	auto materialized = materialized_pending->Execute();
	REQUIRE(materialized->GetResultType() == QueryResultType::ARROW_RESULT);

	// Async with materialized output refuses instead of silently producing an Arrow result
	QueryParameters materialized_async(QueryResultOutputType::FORCE_MATERIALIZED);
	materialized_async.execution_mode = QueryResultExecutionMode::ASYNC;
	auto pending = con.PendingQuery("SELECT i FROM range(10) t(i)", materialized_async);
	REQUIRE(pending->HasError());
	REQUIRE(StringUtil::Contains(pending->GetError(), "streaming result"));

	// Async with streaming output bypasses the hook and keeps the async contract
	auto stream = ExecuteAsync(con, "SELECT i FROM range(10000) t(i)");
	auto deadline = MakeDeadline();
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			row_count += chunk->size();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(row_count == 10000);
}

TEST_CASE("Async stream result drains a streaming-fanout CTE plan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE fanout AS SELECT range i, range % 512 g FROM range(200000)"));

	// Distinct aggregates are rewritten into a shared materialized CTE: one scan fans out to both consumers
	const string query = "SELECT COUNT(DISTINCT i), SUM(DISTINCT g) FROM fanout";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	NotifyListener listener;
	SetDefaultNotifier(con, listener);
	auto stream = ExecuteAsync(con, query);
	auto deadline = MakeDeadline();
	idx_t row_count = 0;
	int64_t count_distinct = -1;
	int64_t sum_distinct = -1;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				count_distinct = chunk->GetValue(0, i).GetValue<int64_t>();
				sum_distinct = chunk->GetValue(1, i).GetValue<int64_t>();
			}
			row_count += chunk->size();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE((execution_result == StreamExecutionResult::BLOCKED ||
		         execution_result == StreamExecutionResult::CHUNK_NOT_READY));
		// Park until the engine wakes us. A missed notification over the fanout plan fails here by timeout.
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	REQUIRE(row_count == 1);
	REQUIRE(count_distinct == 200000);
	REQUIRE(sum_distinct == 130816);
}

TEST_CASE("Abandoning an undrained async stream result unwinds a streaming-fanout plan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Order-destroying settings admit async; the tiny buffer keeps producer sinks parked mid-plan
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	// Two readers over one materialized CTE stream rows through the fanout into the result
	const string query = "WITH c AS MATERIALIZED (SELECT i FROM range(2000000) t(i)) "
	                     "SELECT i FROM c WHERE i % 7 = 0 UNION ALL SELECT i FROM c WHERE i % 11 = 3";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	NotifyListener listener;
	SetDefaultNotifier(con, listener);

	// Abandon at the pending stage: workers already produce in async mode, Execute never runs
	{
		auto pending = con.PendingQuery(query, AsyncParameters());
		if (pending->HasError()) {
			FAIL(pending->GetError());
		}
		// Wait until production demonstrably started, then drop the pending result
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// Abandon mid-stream: fetch a little, then drop the result while producers sit parked behind it
	{
		auto stream = ExecuteAsync(con, query);
		auto deadline = MakeDeadline();
		idx_t row_count = 0;
		while (row_count < 10000) {
			REQUIRE(!DeadlinePassed(deadline));
			unique_ptr<DataChunk> chunk;
			auto execution_result = stream->TryFetchChunk(chunk);
			if (execution_result == StreamExecutionResult::CHUNK_READY) {
				row_count += chunk->size();
				continue;
			}
			REQUIRE(execution_result != StreamExecutionResult::EXECUTION_ERROR);
			REQUIRE(execution_result != StreamExecutionResult::EXECUTION_FINISHED);
			REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
		}
	}
	result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Async batched stream result preserves insertion order", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	// preserve_insertion_order is on by default: a table scan takes the batched streaming collector
	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	REQUIRE_NOTHROW(stream->GetBufferedData().Cast<BatchedBufferedData>());
	auto deadline = MakeDeadline();
	int64_t next_value = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			next_value = VerifyAscending(*chunk, next_value);
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE((execution_result == StreamExecutionResult::BLOCKED ||
		         execution_result == StreamExecutionResult::CHUNK_NOT_READY));
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(next_value == 500000);

	// The connection is usable again after the stream is drained
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Async batched stream result wakes a parked consumer", "[api]") {
	constexpr idx_t ROW_COUNT = 500000;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("CREATE TABLE t AS SELECT range i FROM range(%llu)", ROW_COUNT)));
	// A one-chunk buffer makes the parking structural: the consumer drains to empty and
	// parks between chunks in every build type, so each append-edge wake is exercised
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='1B'"));

	// The callback must never run on the consumer's stack inside an engine call
	StackCheckListener state;
	state.consumer_thread = std::this_thread::get_id();
	std::atomic<idx_t> notification_count {0};
	auto &config = ClientConfig::GetConfig(*con.context);
	config.default_notify_callback = [&state, &notification_count]() {
		notification_count++;
		if (std::this_thread::get_id() == state.consumer_thread && state.in_call.load()) {
			state.consumer_stack_notifications++;
		}
		state.listener.Notify();
	};

	state.in_call = true;
	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	state.in_call = false;

	// Purely notification-driven, like an event loop: the consumer parks unconditionally
	// between drains and only advances when the engine notifies. A missed chunk-arrival
	// or terminal notification fails by timeout.
	auto deadline = MakeDeadline();
	int64_t next_value = 0;
	bool finished = false;
	while (!finished) {
		REQUIRE(!DeadlinePassed(deadline));
		REQUIRE(state.listener.WaitAndReset(std::chrono::seconds(10)));
		while (true) {
			unique_ptr<DataChunk> chunk;
			state.in_call = true;
			auto execution_result = stream->TryFetchChunk(chunk);
			state.in_call = false;
			if (execution_result == StreamExecutionResult::CHUNK_READY) {
				next_value = VerifyAscending(*chunk, next_value);
				continue;
			}
			if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
				finished = true;
			} else {
				REQUIRE((execution_result == StreamExecutionResult::BLOCKED ||
				         execution_result == StreamExecutionResult::CHUNK_NOT_READY));
			}
			break;
		}
	}
	REQUIRE(next_value == int64_t(ROW_COUNT));
	REQUIRE(state.consumer_stack_notifications.load() == 0);
	// At a one-chunk buffer, admission implies an empty queue, so every direct append is
	// an empty-to-non-empty edge and must notify, one per chunk, regardless of consumer
	// timing. Losing the chunk-arrival notification collapses this count.
	INFO(notification_count.load());
	REQUIRE(notification_count.load() >= (ROW_COUNT / STANDARD_VECTOR_SIZE) / 2);
}

TEST_CASE("Async batched stream result wakes on the terminal transition", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE empty_t AS SELECT range i FROM range(0)"));

	NotifyListener listener;
	SetDefaultNotifier(con, listener);
	// A rowless scan produces no append edge; only the terminal notification can wake us
	auto stream = ExecuteAsync(con, "SELECT i FROM empty_t");
	auto deadline = MakeDeadline();
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			REQUIRE(!chunk);
			break;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_ERROR);
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	// The terminal state is sticky: further calls keep reporting it
	unique_ptr<DataChunk> chunk;
	REQUIRE(stream->TryFetchChunk(chunk) == StreamExecutionResult::EXECUTION_FINISHED);
}

TEST_CASE("Async batched stream result surfaces a mid-stream execution error", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream =
	    ExecuteAsync(con, "SELECT CASE WHEN i < 100000 THEN i ELSE CAST(concat('x', i) AS BIGINT) END FROM t");
	auto deadline = MakeDeadline();
	idx_t clean_rows = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::EXECUTION_ERROR) {
			break;
		}
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			clean_rows += chunk->size();
			continue;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_FINISHED);
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(stream->HasError());
	INFO(stream->GetError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "Conversion"));
	// Only rows before the failing one can have been delivered
	REQUIRE(clean_rows <= 100000);
	// The terminal error is sticky: further calls keep reporting it
	unique_ptr<DataChunk> chunk;
	REQUIRE(stream->TryFetchChunk(chunk) == StreamExecutionResult::EXECUTION_ERROR);
}

TEST_CASE("Async batched stream result observes an interrupt", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(4000000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	auto deadline = MakeDeadline();
	// Consume a little, then interrupt while sinks are parked in both tiers
	int64_t next_value = 0;
	while (next_value < 10000) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			next_value = VerifyAscending(*chunk, next_value);
			continue;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_ERROR);
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_FINISHED);
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	con.Interrupt();
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::EXECUTION_ERROR) {
			break;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_FINISHED);
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(stream->HasError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "INTERRUPT"));
	// Every parked producer unwound: the connection is usable again
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Async batched stream result under batch skew", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Many row groups and a small buffer: high batches complete while the minimum lags
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE skew AS SELECT range i FROM range(2000000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='64KB'"));

	NotifyListener listener;
	SetDefaultNotifier(con, listener);
	auto stream = ExecuteAsync(con, "SELECT i FROM skew");
	auto deadline = MakeDeadline();
	int64_t next_value = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			next_value = VerifyAscending(*chunk, next_value);
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE((execution_result == StreamExecutionResult::BLOCKED ||
		         execution_result == StreamExecutionResult::CHUNK_NOT_READY));
		// A lost wake under skewed batch completion fails here by timeout
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	REQUIRE(next_value == 2000000);
}

TEST_CASE("Async batched notifications fire only when the queue turns non-empty", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// A buffer far larger than the whole result: no producer ever parks, and until the
	// consumer pops, the read queue turns non-empty exactly once. Legitimate fires are
	// that one append edge plus a bounded terminal tail, on any machine at any vector
	// size; a notifier that fired per append would fire once per chunk.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100MB'"));
	// Bounds the terminal tail: each worker can observe the finished state once
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));

	NotifyListener listener;
	std::atomic<idx_t> notification_count {0};
	auto &config = ClientConfig::GetConfig(*con.context);
	config.default_notify_callback = [&listener, &notification_count]() {
		notification_count++;
		listener.Notify();
	};

	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	// Do not pop. With a buffer this large the collector never blocks, so ExecuteAsync
	// returns only after the whole query completed; the wait below merely flushes
	// straggler terminal notifications, and an early quiet verdict can only lower the
	// count before the assertion, never raise it.
	auto deadline = MakeDeadline();
	REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	while (listener.WaitAndReset(std::chrono::milliseconds(500))) {
		REQUIRE(!DeadlinePassed(deadline));
	}
	// Pins the append notify site; the move site contributes at most the one first-fill
	// fire here (two row groups), and is exercised under parking by the skew test
	const auto observed_notifications = notification_count.load();
	INFO(observed_notifications);
	REQUIRE(observed_notifications <= 16);

	// The result is intact: drain and verify
	int64_t next_value = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			next_value = VerifyAscending(*chunk, next_value);
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(next_value == 200000);
}

TEST_CASE("Async batched stream result survives tiny stream buffers", "[api]") {
	for (auto buffer_size : {"0B", "1B"}) {
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(250000)"));
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("SET streaming_buffer_size='%s'", buffer_size)));

		auto stream = ExecuteAsync(con, "SELECT i FROM t");
		auto deadline = MakeDeadline();
		int64_t next_value = 0;
		while (true) {
			REQUIRE(!DeadlinePassed(deadline));
			unique_ptr<DataChunk> chunk;
			auto execution_result = stream->TryFetchChunk(chunk);
			if (execution_result == StreamExecutionResult::CHUNK_READY) {
				next_value = VerifyAscending(*chunk, next_value);
				continue;
			}
			if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
				break;
			}
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		}
		REQUIRE(next_value == 250000);

		// Sync leg: with both tiers floored at one byte the buffer still admits chunks.
		// Before the floors this parked every sink on an always-full empty tier, an
		// unbounded busy loop; the watchdog interrupt turns that regression into an error.
		std::atomic<bool> sync_leg_done {false};
		std::thread watchdog([&]() {
			auto watchdog_deadline = MakeDeadline();
			while (!sync_leg_done.load() && !DeadlinePassed(watchdog_deadline)) {
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
			}
			if (!sync_leg_done.load()) {
				con.Interrupt();
			}
		});
		unique_ptr<MaterializedQueryResult> materialized;
		try {
			auto pending = con.PendingQuery("SELECT i FROM t", QueryParameters(true));
			if (!pending->HasError()) {
				auto result = pending->Execute();
				if (result->GetResultType() == QueryResultType::STREAM_RESULT) {
					materialized = result->Cast<StreamQueryResult>().Materialize();
				}
			}
		} catch (...) {
			// The watchdog must be joined on every path, or the thread dtor aborts
			sync_leg_done = true;
			watchdog.join();
			throw;
		}
		sync_leg_done = true;
		watchdog.join();
		REQUIRE(materialized);
		REQUIRE(!materialized->HasError());
		REQUIRE(materialized->RowCount() == 250000);
	}
}

TEST_CASE("Blocking materialization of an async batched stream result", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	auto materialized = stream->Materialize();
	REQUIRE(!materialized->HasError());
	REQUIRE(materialized->RowCount() == 500000);
	REQUIRE(materialized->GetValue(0, 0).GetValue<int64_t>() == 0);
	REQUIRE(materialized->GetValue(0, 499999).GetValue<int64_t>() == 499999);
}

TEST_CASE("Async batched stream drain across buffer sizes", "[api]") {
	// Stresses the restart and notify edges across buffer sizes: with a small buffer,
	// faster batches fill the in-progress tier while the minimum batch lags. A lost
	// restart or notification edge fails by timeout.
	auto drain_counting_moved_nothing = [](const char *buffer_size, idx_t row_count) -> idx_t {
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("CREATE TABLE t AS SELECT range i FROM range(%llu)", row_count)));
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("SET streaming_buffer_size='%s'", buffer_size)));

		NotifyListener listener;
		SetDefaultNotifier(con, listener);
		auto stream = ExecuteAsync(con, "SELECT i FROM t");
		auto deadline = MakeDeadline();
		int64_t next_value = 0;
		while (true) {
			REQUIRE(!DeadlinePassed(deadline));
			unique_ptr<DataChunk> chunk;
			auto execution_result = stream->TryFetchChunk(chunk);
			if (execution_result == StreamExecutionResult::CHUNK_READY) {
				next_value = VerifyAscending(*chunk, next_value);
				continue;
			}
			if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
				break;
			}
			// A lost restart or notification edge fails here by timeout
			REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
		}
		REQUIRE(next_value == int64_t(row_count));
		return stream->GetBufferedData().Cast<BatchedBufferedData>().MovedNothingRestarts();
	};

	// Only the 1KB drain pins the deadlock shape at the default vector size: the queue
	// capacity is fixed while the chunk cost scales with STANDARD_VECTOR_SIZE, so here
	// "below capacity" means empty and the pop edge cannot rescue a parked minimum
	// sink. The larger sizes are stress only.
	idx_t moved_nothing_restarts = drain_counting_moved_nothing("1KB", 1000000);
	(void)drain_counting_moved_nothing("32KB", 1000000);
	(void)drain_counting_moved_nothing("256KB", 1000000);

	// The moved-nothing advance (a newly minimum batch that parked before its first
	// append) is scheduling-dependent. Hunt for it on a small table (the shape needs a
	// few row groups in flight), bounded by attempts. The counter's job is to stop the
	// hunt early on healthy code; under the freed-bytes regression it stays zero, every
	// attempt runs, and any drain that hits the shape hangs on the 10 second waits and
	// fails. A zero result is reported for direct local runs only, never a failure.
	for (idx_t attempt = 0; moved_nothing_restarts == 0 && attempt < 6; attempt++) {
		moved_nothing_restarts += drain_counting_moved_nothing("1KB", 4 * DEFAULT_ROW_GROUP_SIZE);
	}
	if (moved_nothing_restarts == 0) {
		WARN("moved-nothing advance not observed in this run");
	}
}

TEST_CASE("Async batched stream result drains a streaming-fanout CTE plan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	// Two readers over one table-backed materialized CTE: the batch index survives the
	// exchange, so the order-preserving batched collector consumes fanout output directly
	const string query = "WITH c AS MATERIALIZED (SELECT i FROM t) SELECT (SELECT COUNT(*) FROM c) total, i FROM c";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	NotifyListener listener;
	SetDefaultNotifier(con, listener);
	auto stream = ExecuteAsync(con, query);
	// PIPELINE_DEPENDENT alone does not pin the collector: without a batch index this
	// plan would silently fall back to the order-preserving simple collector
	REQUIRE_NOTHROW(stream->GetBufferedData().Cast<BatchedBufferedData>());
	auto deadline = MakeDeadline();
	int64_t next_value = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			chunk->Flatten();
			auto totals = FlatVector::GetData<int64_t>(chunk->data[0]);
			for (idx_t i = 0; i < chunk->size(); i++) {
				if (totals[i] != 500000) {
					FAIL(StringUtil::Format("Wrong total: %lld", totals[i]));
				}
			}
			next_value = VerifyAscending(*chunk, next_value, 1);
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE((execution_result == StreamExecutionResult::BLOCKED ||
		         execution_result == StreamExecutionResult::CHUNK_NOT_READY));
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	REQUIRE(next_value == 500000);
}

TEST_CASE("Abandoning an undrained async batched stream result unwinds a streaming-fanout plan", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(2000000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	const string query = "WITH c AS MATERIALIZED (SELECT i FROM t) SELECT (SELECT COUNT(*) FROM c) total, i FROM c";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	NotifyListener listener;
	SetDefaultNotifier(con, listener);

	// Abandon at the pending stage: workers already produce in async mode, Execute never runs
	{
		auto pending = con.PendingQuery(query, AsyncParameters());
		if (pending->HasError()) {
			FAIL(pending->GetError());
		}
		// Wait until production demonstrably started, then drop the pending result
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// Abandon mid-stream: fetch a little, then drop the result while producers sit parked
	{
		auto stream = ExecuteAsync(con, query);
		REQUIRE_NOTHROW(stream->GetBufferedData().Cast<BatchedBufferedData>());
		auto deadline = MakeDeadline();
		idx_t row_count = 0;
		while (row_count < 10000) {
			REQUIRE(!DeadlinePassed(deadline));
			unique_ptr<DataChunk> chunk;
			auto execution_result = stream->TryFetchChunk(chunk);
			if (execution_result == StreamExecutionResult::CHUNK_READY) {
				row_count += chunk->size();
				continue;
			}
			REQUIRE(execution_result != StreamExecutionResult::EXECUTION_ERROR);
			REQUIRE(execution_result != StreamExecutionResult::EXECUTION_FINISHED);
			REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
		}
	}
	result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("Async stream result with an empty result", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto stream = ExecuteAsync(con, "SELECT i FROM range(0) t(i)");
	auto deadline = MakeDeadline();
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			REQUIRE(!chunk);
			break;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_ERROR);
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	// The terminal state is sticky: further calls keep reporting it
	unique_ptr<DataChunk> chunk;
	REQUIRE(stream->TryFetchChunk(chunk) == StreamExecutionResult::EXECUTION_FINISHED);
	REQUIRE(stream->TryFetchChunk(chunk) == StreamExecutionResult::EXECUTION_FINISHED);
}

TEST_CASE("Async stream result surfaces a mid-stream execution error", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(
	    con, "SELECT CASE WHEN i < 100000 THEN i ELSE CAST(concat('x', i) AS BIGINT) END FROM range(200000) t(i)");
	auto deadline = MakeDeadline();
	idx_t clean_rows = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::EXECUTION_ERROR) {
			break;
		}
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			clean_rows += chunk->size();
			continue;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_FINISHED);
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(stream->HasError());
	INFO(stream->GetError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "Conversion"));
	// Only rows before the failing one can have been delivered
	REQUIRE(clean_rows <= 100000);
	// The terminal error is sticky: further calls keep reporting it
	unique_ptr<DataChunk> chunk;
	REQUIRE(stream->TryFetchChunk(chunk) == StreamExecutionResult::EXECUTION_ERROR);
}

TEST_CASE("Async stream result observes an interrupt", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM range(100000000) t(i)");
	auto deadline = MakeDeadline();
	// Consume a little, then interrupt
	idx_t rows_before_interrupt = 0;
	while (rows_before_interrupt < 10000) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			rows_before_interrupt += chunk->size();
			continue;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_ERROR);
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_FINISHED);
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	con.Interrupt();
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::EXECUTION_ERROR) {
			break;
		}
		REQUIRE(execution_result != StreamExecutionResult::EXECUTION_FINISHED);
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(stream->HasError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "INTERRUPT"));
}

TEST_CASE("Async stream results refuse a single-threaded database", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));

	auto pending = con.PendingQuery("SELECT i FROM range(10) t(i)", AsyncParameters());
	REQUIRE(pending->HasError());
	REQUIRE(StringUtil::Contains(pending->GetError(), "more than one thread"));
}

TEST_CASE("Async stream results refuse an external-threads-only scheduler", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	// The pool total counts external threads, which never pump the queue on their own
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=4"));

	auto pending = con.PendingQuery("SELECT i FROM range(10) t(i)", AsyncParameters());
	REQUIRE(pending->HasError());
	REQUIRE(StringUtil::Contains(pending->GetError(), "external"));
}

TEST_CASE("Async stream results require a streaming result", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Materialized output cannot be async
	QueryParameters materialized(QueryResultOutputType::FORCE_MATERIALIZED);
	materialized.execution_mode = QueryResultExecutionMode::ASYNC;
	auto pending = con.PendingQuery("SELECT i FROM range(10) t(i)", materialized);
	REQUIRE(pending->HasError());
	REQUIRE(StringUtil::Contains(pending->GetError(), "streaming result"));

	// A statement type that cannot stream cannot be async either
	pending = con.PendingQuery("CREATE TABLE integers(i INTEGER)", AsyncParameters());
	REQUIRE(pending->HasError());
	REQUIRE(StringUtil::Contains(pending->GetError(), "streaming result"));
}

TEST_CASE("Async stream result on a table scan without insertion-order preservation", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i FROM range(10000) t(i)"));

	// Without insertion-order preservation the plain streaming collector serves a table scan
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	auto deadline = MakeDeadline();
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!DeadlinePassed(deadline));
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			row_count += chunk->size();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(row_count == 10000);
}

TEST_CASE("TryFetchChunk refuses a sync stream result", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto pending = con.PendingQuery("SELECT i FROM range(10) t(i)", QueryParameters(true));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	auto &stream = result->Cast<StreamQueryResult>();
	unique_ptr<DataChunk> chunk;
	REQUIRE_THROWS(stream.TryFetchChunk(chunk));
}

#else

TEST_CASE("Async stream results refuse a threadless build", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto pending = con.PendingQuery("SELECT i FROM range(10) t(i)", AsyncParameters());
	REQUIRE(pending->HasError());
}

#endif
