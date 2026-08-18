#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/arrow/physical_arrow_collector.hpp"
#include "duckdb/common/query_parameters.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/stream_query_result.hpp"

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

TEST_CASE("Async stream results refuse order-preserving batched plans", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i FROM range(10000) t(i)"));

	// preserve_insertion_order is on by default: a table scan takes the batched streaming collector
	auto pending = con.PendingQuery("SELECT i FROM t", AsyncParameters());
	REQUIRE(pending->HasError());
	REQUIRE(StringUtil::Contains(pending->GetError(), "preserve_insertion_order"));

	// Without insertion-order preservation the plain streaming collector is used and driving works
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
