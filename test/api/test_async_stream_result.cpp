#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/arrow/physical_arrow_collector.hpp"
#include "duckdb/common/query_parameters.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/storage/storage_info.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <thread>

using namespace duckdb;

namespace {

//! A listener the callback wakes (Notify() -> cv.notify_one()). The consumer waits on it.
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

//! Bounds every drain loop, so a liveness bug fails the test instead of hanging the suite
struct Deadline {
	std::chrono::steady_clock::time_point expiry = std::chrono::steady_clock::now() + std::chrono::seconds(60);

	bool Passed() const {
		return std::chrono::steady_clock::now() >= expiry;
	}
};

//! Interrupts the connection when the guarded scope outlives the deadline, so a hung
//! blocking drain fails its test instead of hanging the suite
class DrainWatchdog {
public:
	explicit DrainWatchdog(Connection &con)
	    : watcher([this, &con]() {
		      Deadline deadline;
		      while (!done.load() && !deadline.Passed()) {
			      std::this_thread::sleep_for(std::chrono::milliseconds(100));
		      }
		      if (!done.load()) {
			      con.Interrupt();
		      }
	      }) {
	}
	~DrainWatchdog() {
		done = true;
		watcher.join();
	}

private:
	std::atomic<bool> done {false};
	std::thread watcher;
};

//! Switch the connection to asynchronous streaming result consumption
void EnableAsync(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("SET streaming_execution_mode='async'"));
}

unique_ptr<StreamQueryResult> ExecuteAsync(Connection &con, const string &query) {
	EnableAsync(con);
	auto pending = con.PendingQuery(query, QueryParameters(true));
	if (pending->HasError()) {
		FAIL(pending->GetError());
	}
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	return unique_ptr_cast<QueryResult, StreamQueryResult>(std::move(result));
}

//! Install the connection notifier that wakes the listener
void SetNotifier(Connection &con, NotifyListener &listener) {
	auto &config = ClientConfig::GetConfig(*con.context);
	config.notify_callback = [&listener]() {
		listener.Notify();
	};
}

//! Install a notifier that also counts the notifications it delivers
void SetCountingNotifier(Connection &con, NotifyListener &listener, std::atomic<idx_t> &count) {
	auto &config = ClientConfig::GetConfig(*con.context);
	config.notify_callback = [&listener, &count]() {
		count++;
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

//! Verify a chunk column continues the ascending sequence. Returns the next expected value.
//! Reads the vector data directly: the consumer must stay fast enough to outrun the
//! producers, or the drain loops never wait and the wake paths go untested.
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

//! Counting listener that also records callbacks running on the consumer's own stack
//! inside an engine call. Wrap engine calls in in_call to enable the check.
struct StackCheckListener {
	NotifyListener listener;
	std::atomic<bool> in_call {false};
	std::atomic<idx_t> consumer_stack_notifications {0};
	std::atomic<idx_t> notification_count {0};
	std::thread::id consumer_thread;

	//! Install as the connection notifier. Records the calling thread as the consumer
	void Install(Connection &con) {
		consumer_thread = std::this_thread::get_id();
		auto &config = ClientConfig::GetConfig(*con.context);
		config.notify_callback = [this]() {
			notification_count++;
			if (std::this_thread::get_id() == consumer_thread && in_call.load()) {
				consumer_stack_notifications++;
			}
			listener.Notify();
		};
	}
};

} // namespace

#ifndef DUCKDB_NO_THREADS

// Core contract: a full drain that never blocks and never executes engine work

TEST_CASE("Async stream result completes using TryFetch only", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// A tiny buffer forces many block/restart cycles on the producer sinks
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM range(500000) t(i)");
	Deadline deadline;
	idx_t row_count = 0;
	int64_t sum = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("A non-blocking pending consumer progresses on notifications alone", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));
	EnableAsync(con);

	// The callback is read at submission, so install it first
	NotifyListener listener;
	SetNotifier(con, listener);

	auto pending = con.PendingQuery("SELECT i FROM range(500000) t(i)", QueryParameters(true));
	REQUIRE(!pending->HasError());

	// Drive the pending phase the way an event-loop consumer does: observe with
	// ExecuteTask, and sleep on the callback whenever no progress is possible. An
	// observable chunk must make ExecuteTask report the result ready. Before that,
	// readiness required a blocked producer, a transition with no notification, so
	// this loop consumed the first-append notification and slept forever.
	Deadline deadline;
	while (true) {
		REQUIRE(!deadline.Passed());
		auto execution_result = pending->ExecuteTask();
		if (PendingQueryResult::IsResultReady(execution_result)) {
			break;
		}
		REQUIRE(execution_result != PendingExecutionResult::EXECUTION_ERROR);
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	auto &stream = result->Cast<StreamQueryResult>();

	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream.TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			row_count += chunk->size();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	REQUIRE(row_count == 500000);
}

TEST_CASE("TryFetchChunk refuses a sync stream result", "[api][async_stream]") {
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

// Wake-ups: a waiting consumer is always woken, and never on its own stack

TEST_CASE("Async stream result wakes a waiting consumer", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	// The callback must never run on the consumer's stack inside an engine call
	StackCheckListener state;
	state.Install(con);

	state.in_call = true;
	auto stream = ExecuteAsync(con, "SELECT i FROM range(500000) t(i)");
	state.in_call = false;

	Deadline deadline;
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async batched stream result wakes a waiting consumer", "[api][async_stream]") {
	constexpr idx_t ROW_COUNT = 500000;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("CREATE TABLE t AS SELECT range i FROM range(%llu)", ROW_COUNT)));
	// A one-chunk buffer forces waiting. The consumer drains to empty and waits between
	// chunks in every build type, so each append wake is exercised
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='1B'"));

	// The callback must never run on the consumer's stack inside an engine call
	StackCheckListener state;
	state.Install(con);

	state.in_call = true;
	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	state.in_call = false;

	// Purely notification-driven. The consumer waits unconditionally
	// between drains and only advances when the engine notifies. A missed chunk-arrival
	// or terminal notification fails by timeout.
	Deadline deadline;
	int64_t next_value = 0;
	bool finished = false;
	while (!finished) {
		REQUIRE(!deadline.Passed());
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
	// A chunk can arrive through the consumer's own restart selection, which deposits a
	// parked chunk during the pop. Those arrivals are self-observed and must not notify:
	// the callback never runs on the consumer's stack. Only appends into a queue the
	// consumer may be sleeping on notify, so the count depends on timing. Delivery is
	// pinned by the bounded waits above. The initial append always notifies.
	INFO(state.notification_count.load());
	REQUIRE(state.notification_count.load() >= 1);
}

TEST_CASE("Async stream result wakes the consumer on the terminal transition", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Produces no rows, so the only notification can come from execution finishing
	NotifyListener listener;
	SetNotifier(con, listener);
	auto stream = ExecuteAsync(con, "SELECT i FROM range(50000000) t(i) WHERE i < 0");

	Deadline deadline;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async batched stream result wakes on the terminal transition", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE empty_t AS SELECT range i FROM range(0)"));

	NotifyListener listener;
	SetNotifier(con, listener);
	// A rowless scan never appends. Only the terminal notification can wake us
	auto stream = ExecuteAsync(con, "SELECT i FROM empty_t");
	Deadline deadline;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async stream result wakes a waiting consumer on interrupt", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	NotifyListener listener;
	SetNotifier(con, listener);
	auto stream = ExecuteAsync(con, "SELECT i FROM range(100000000) t(i)");

	// Deliberately waits without fetching, to pin the one case where only the interrupt
	// notification can wake us: producers blocked on a full buffer. The loop tolerates the
	// few early append notifications.
	std::thread interruptor([&con]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
		con.Interrupt();
	});
	Deadline deadline;
	while (true) {
		REQUIRE(!deadline.Passed());
		if (!listener.WaitAndReset(std::chrono::seconds(10))) {
			FAIL("the interrupt did not wake the waiting consumer");
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

TEST_CASE("The connection notifier observes completion without fetching", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	NotifyListener listener;
	SetNotifier(con, listener);

	EnableAsync(con);
	auto pending = con.PendingQuery("SELECT 42", QueryParameters(true));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	// The notifier was set before execution started, so the first chunk or the terminal
	// transition must notify even though nothing has been fetched yet
	REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));

	auto &stream = result->Cast<StreamQueryResult>();
	Deadline deadline;
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

// Notification discipline: fire on the empty-to-non-empty transition, and only there

TEST_CASE("Async batched notifications fire only when the queue turns non-empty", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// A buffer far larger than the whole result: no producer ever blocks, and until the
	// consumer pops, the read queue turns non-empty exactly once. Legitimate fires are
	// that one transition plus a bounded terminal tail, on any machine at any vector
	// size. A notifier that fired per append would fire once per chunk.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100MB'"));
	// Bounds the terminal tail: each worker can observe the finished state once
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));

	NotifyListener listener;
	std::atomic<idx_t> notification_count {0};
	SetCountingNotifier(con, listener, notification_count);

	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	// Do not pop. With a buffer this large the collector never blocks, so ExecuteAsync
	// returns only after the whole query completed. The wait below merely flushes
	// straggler terminal notifications, and an early quiet verdict can only lower the
	// count before the assertion, never raise it.
	Deadline deadline;
	REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	while (listener.WaitAndReset(std::chrono::milliseconds(500))) {
		REQUIRE(!deadline.Passed());
	}
	// Pins the append notify site. The move site contributes at most the one first-fill
	// fire here (two row groups), and is exercised under blocking by the skew test
	const auto observed_notifications = notification_count.load();
	INFO(observed_notifications);
	REQUIRE(observed_notifications <= 16);

	// The result is intact: drain and verify
	int64_t next_value = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async stream result wakes on chunks with rows but no data bytes", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Empty-struct chunks have rows but zero data bytes: readiness and notification
	// must both follow the chunk queue, not the byte count
	NotifyListener listener;
	SetNotifier(con, listener);
	auto stream = ExecuteAsync(con, "SELECT struct_pack() FROM range(100000) t(i)");

	Deadline deadline;
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

// Order: the batched collector's reason to exist

TEST_CASE("Async batched stream result preserves insertion order", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	// preserve_insertion_order is on by default: a table scan takes the batched streaming collector
	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	REQUIRE_NOTHROW(stream->GetBufferedData().Cast<BatchedBufferedData>());
	Deadline deadline;
	int64_t next_value = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async batched stream result under batch skew", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Many row groups and a small buffer: high batches complete while the minimum lags
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE skew AS SELECT range i FROM range(2000000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='64KB'"));

	NotifyListener listener;
	SetNotifier(con, listener);
	auto stream = ExecuteAsync(con, "SELECT i FROM skew");
	Deadline deadline;
	int64_t next_value = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async batched stream drain across buffer sizes", "[api][async_stream]") {
	// Stresses restarts and notifications across buffer sizes: with a small buffer,
	// faster batches fill the in-progress buffer while the minimum batch lags. A lost
	// restart or notification fails by timeout.
	auto drain_counting_moved_nothing = [](const char *buffer_size, idx_t row_count) -> idx_t {
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("CREATE TABLE t AS SELECT range i FROM range(%llu)", row_count)));
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("SET streaming_buffer_size='%s'", buffer_size)));

		NotifyListener listener;
		SetNotifier(con, listener);
		auto stream = ExecuteAsync(con, "SELECT i FROM t");
		Deadline deadline;
		int64_t next_value = 0;
		while (true) {
			REQUIRE(!deadline.Passed());
			unique_ptr<DataChunk> chunk;
			auto execution_result = stream->TryFetchChunk(chunk);
			if (execution_result == StreamExecutionResult::CHUNK_READY) {
				next_value = VerifyAscending(*chunk, next_value);
				continue;
			}
			if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
				break;
			}
			// A lost restart or notification fails here by timeout
			REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
		}
		REQUIRE(next_value == int64_t(row_count));
		return stream->GetBufferedData().Cast<BatchedBufferedData>().MovedNothingRestarts();
	};

	// Only the 1KB drain pins the deadlock shape at the default vector size: the queue
	// capacity is fixed while the chunk cost scales with STANDARD_VECTOR_SIZE, so here
	// "below capacity" means empty and a pop cannot rescue a blocked minimum
	// sink. The larger sizes are stress only.
	idx_t moved_nothing_restarts = drain_counting_moved_nothing("1KB", 1000000);
	(void)drain_counting_moved_nothing("32KB", 1000000);
	(void)drain_counting_moved_nothing("256KB", 1000000);

	// The moved-nothing advance (a newly minimum batch that blocked before its first
	// append) is scheduling-dependent. Hunt for it on a small table (the shape needs a
	// few row groups in flight), bounded by attempts. The counter's job is to stop the
	// hunt early on healthy code. Under the freed-bytes regression it stays zero, every
	// attempt runs, and any drain that hits the shape hangs on the 10 second waits and
	// fails. A zero result is reported for direct local runs only, never a failure.
	for (idx_t attempt = 0; moved_nothing_restarts == 0 && attempt < 6; attempt++) {
		moved_nothing_restarts += drain_counting_moved_nothing("1KB", 4 * DEFAULT_ROW_GROUP_SIZE);
	}
	if (moved_nothing_restarts == 0) {
		WARN("moved-nothing advance not observed in this run");
	}
}

// Endings: one terminal state per query, visible to the consumer and sticky

TEST_CASE("Async stream result with an empty result", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto stream = ExecuteAsync(con, "SELECT i FROM range(0) t(i)");
	Deadline deadline;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async stream result surfaces a mid-stream execution error", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	// The error-side terminal notify (PushError) must never run on the consumer's stack
	StackCheckListener state;
	state.Install(con);

	state.in_call = true;
	auto stream = ExecuteAsync(
	    con, "SELECT CASE WHEN i < 100000 THEN i ELSE CAST(concat('x', i) AS BIGINT) END FROM range(200000) t(i)");
	state.in_call = false;
	Deadline deadline;
	idx_t clean_rows = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
		unique_ptr<DataChunk> chunk;
		state.in_call = true;
		auto execution_result = stream->TryFetchChunk(chunk);
		state.in_call = false;
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
	REQUIRE(state.consumer_stack_notifications.load() == 0);
	REQUIRE(stream->HasError());
	INFO(stream->GetError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "Conversion"));
	// Only rows before the failing one can have been delivered
	REQUIRE(clean_rows <= 100000);
	// The terminal error is sticky: further calls keep reporting it
	unique_ptr<DataChunk> chunk;
	REQUIRE(stream->TryFetchChunk(chunk) == StreamExecutionResult::EXECUTION_ERROR);
}

TEST_CASE("Async batched stream result surfaces a mid-stream execution error", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream =
	    ExecuteAsync(con, "SELECT CASE WHEN i < 100000 THEN i ELSE CAST(concat('x', i) AS BIGINT) END FROM t");
	Deadline deadline;
	idx_t clean_rows = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

//! Throws a catalog error once the input values pass 100000, exercising a mid-stream
//! failure with an exception type that Exception::InvalidatesTransaction exempts
static void ThresholdBoomExec(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<int64_t, int64_t>(args.data[0], result, args.size(), [](int64_t input) {
		if (input >= 100000) {
			throw CatalogException("boom past the threshold");
		}
		return input;
	});
}

TEST_CASE("A mid-stream fetch failure invalidates the transaction", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Single-threaded, so the consumer executes the failing task inline and the error
	// type reaches the fetch deterministically, instead of racing the interrupt flag
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));
	ScalarFunction boom("threshold_boom", {LogicalType::BIGINT}, LogicalType::BIGINT, ThresholdBoomExec);
	CreateScalarFunctionInfo info(boom);
	con.context->RunFunctionInTransaction(
	    [&]() { Catalog::GetSystemCatalog(*con.context).CreateFunction(*con.context, info); });

	// A catalog error is exempt from Exception::InvalidatesTransaction, but a fetch
	// failure follows the context policy, like every other error site
	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	auto stream = con.SendQuery("SELECT threshold_boom(i) FROM range(200000) t(i)", QueryParameters(true));
	REQUIRE(!stream->HasError());
	DrainWatchdog watchdog(con);
	while (auto chunk = stream->Fetch()) {
	}
	REQUIRE(stream->HasError());
	INFO(stream->GetError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "boom past the threshold"));

	auto follow_up = con.Query("SELECT 1");
	REQUIRE(follow_up->HasError());
	REQUIRE(StringUtil::Contains(follow_up->GetError(), "aborted"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	REQUIRE_NO_FAIL(con.Query("SELECT 1"));
}

TEST_CASE("Async stream result observes an interrupt", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM range(100000000) t(i)");
	Deadline deadline;
	// Consume a little, then interrupt
	idx_t rows_before_interrupt = 0;
	while (rows_before_interrupt < 10000) {
		REQUIRE(!deadline.Passed());
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
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async batched stream result observes an interrupt", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(4000000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	Deadline deadline;
	// Consume a little, then interrupt while sinks are blocked in both buffers
	int64_t next_value = 0;
	while (next_value < 10000) {
		REQUIRE(!deadline.Passed());
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
		REQUIRE(!deadline.Passed());
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
	// Every blocked producer unwound: the connection is usable again
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

// Cleanup: abandoning an undrained result unwinds the plan

TEST_CASE("Abandoning an undrained async stream result unwinds a streaming-fanout plan", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Order-destroying settings admit async. The tiny buffer keeps producer sinks blocked mid-plan
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	// Two readers over one materialized CTE stream rows through the fanout into the result
	const string query = "WITH c AS MATERIALIZED (SELECT i FROM range(2000000) t(i)) "
	                     "SELECT i FROM c WHERE i % 7 = 0 UNION ALL SELECT i FROM c WHERE i % 11 = 3";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	NotifyListener listener;
	SetNotifier(con, listener);

	// Abandon at the pending stage: workers already produce in async mode, Execute never runs
	{
		EnableAsync(con);
		auto pending = con.PendingQuery(query, QueryParameters(true));
		if (pending->HasError()) {
			FAIL(pending->GetError());
		}
		// Wait until production demonstrably started, then drop the pending result
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// Abandon mid-stream: fetch a little, then drop the result while producers sit blocked behind it
	{
		auto stream = ExecuteAsync(con, query);
		Deadline deadline;
		idx_t row_count = 0;
		while (row_count < 10000) {
			REQUIRE(!deadline.Passed());
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

TEST_CASE("Abandoning an undrained async batched stream result unwinds a streaming-fanout plan",
          "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(2000000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	const string query = "WITH c AS MATERIALIZED (SELECT i FROM t) SELECT (SELECT COUNT(*) FROM c) total, i FROM c";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	NotifyListener listener;
	SetNotifier(con, listener);

	// Abandon at the pending stage: workers already produce in async mode, Execute never runs
	{
		EnableAsync(con);
		auto pending = con.PendingQuery(query, QueryParameters(true));
		if (pending->HasError()) {
			FAIL(pending->GetError());
		}
		// Wait until production demonstrably started, then drop the pending result
		REQUIRE(listener.WaitAndReset(std::chrono::seconds(10)));
	}
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// Abandon mid-stream: fetch a little, then drop the result while producers sit blocked
	{
		auto stream = ExecuteAsync(con, query);
		REQUIRE_NOTHROW(stream->GetBufferedData().Cast<BatchedBufferedData>());
		Deadline deadline;
		idx_t row_count = 0;
		while (row_count < 10000) {
			REQUIRE(!deadline.Passed());
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

// Refusals: configurations async cannot serve are rejected at submission

TEST_CASE("The async setting refuses a single-threaded database", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));

	auto result = con.Query("SET streaming_execution_mode='async'");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "worker thread"));
	// The refusal has no side effects
	auto setting = con.Query("SELECT current_setting('streaming_execution_mode')");
	REQUIRE(CHECK_COLUMN(setting, 0, {"sync"}));
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	EnableAsync(con);
}

TEST_CASE("The async setting refuses an external-threads-only scheduler", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// External threads never execute tasks on their own
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("SET external_threads=4"));

	auto result = con.Query("SET streaming_execution_mode='async'");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "worker thread"));
}

TEST_CASE("Changing threads is refused while an async connection exists", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection consumer(db);
	Connection other(db);
	EnableAsync(consumer);

	// No connection can remove the last managed worker while an async connection exists
	auto result = other.Query("SET threads=1");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "async"));
	REQUIRE_NO_FAIL(other.Query("SET threads=4"));
	result = other.Query("SET external_threads=4");
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(), "async"));

	// Shrinking while a worker remains is fine
	REQUIRE_NO_FAIL(other.Query("SET threads=2"));

	// Leaving async mode releases the restriction
	REQUIRE_NO_FAIL(consumer.Query("SET streaming_execution_mode='sync'"));
	REQUIRE_NO_FAIL(other.Query("SET threads=1"));
	REQUIRE_NO_FAIL(other.Query("SET threads=4"));

	// So does closing the async connection
	{
		Connection transient(db);
		EnableAsync(transient);
		result = other.Query("SET threads=1");
		REQUIRE(result->HasError());
	}
	REQUIRE_NO_FAIL(other.Query("SET threads=1"));
}

TEST_CASE("Async works with one managed worker and no external threads", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Newly allowed: the invariant is one managed worker, not threads > 1
	REQUIRE_NO_FAIL(con.Query("SET external_threads=0"));
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	EnableAsync(con);

	auto stream = ExecuteAsync(con, "SELECT i FROM range(100000) t(i)");
	Deadline deadline;
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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
	REQUIRE(row_count == 100000);
}

TEST_CASE("The async setting is connection-local", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection async_con(db);
	Connection sync_con(db);
	EnableAsync(async_con);

	// The other connection's streams stay sync: TryFetchChunk refuses them
	auto sync_stream = sync_con.SendQuery("SELECT i FROM range(1000) t(i)", QueryParameters(true));
	REQUIRE(!sync_stream->HasError());
	REQUIRE(sync_stream->GetResultType() == QueryResultType::STREAM_RESULT);
	unique_ptr<DataChunk> chunk;
	REQUIRE_THROWS(sync_stream->Cast<StreamQueryResult>().TryFetchChunk(chunk));
	DrainWatchdog watchdog(sync_con);
	idx_t sync_rows = 0;
	while (auto sync_chunk = sync_stream->Fetch()) {
		sync_rows += sync_chunk->size();
	}
	REQUIRE(sync_rows == 1000);

	// The async connection's stream is async
	auto stream = ExecuteAsync(async_con, "SELECT i FROM range(1000) t(i)");
	Deadline deadline;
	idx_t async_rows = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
		unique_ptr<DataChunk> async_chunk;
		auto execution_result = stream->TryFetchChunk(async_chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			async_rows += async_chunk->size();
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(async_rows == 1000);
}

TEST_CASE("The async setting is ignored for non-streaming results", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_execution_mode='async'"));

	// Materialized output runs sync under the async setting
	auto pending =
	    con.PendingQuery("SELECT i FROM range(10) t(i)", QueryParameters(QueryResultOutputType::FORCE_MATERIALIZED));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::MATERIALIZED_RESULT);
	REQUIRE(result->Cast<MaterializedQueryResult>().RowCount() == 10);

	// So does a statement type that cannot stream
	pending = con.PendingQuery("CREATE TABLE integers(i INTEGER)", QueryParameters(true));
	REQUIRE(!pending->HasError());
	REQUIRE(!pending->Execute()->HasError());
}

// Coexistence: async must not disturb sync, multi-statement, or custom collectors

TEST_CASE("Sync queries ignore the connection notifier", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	NotifyListener listener;
	SetNotifier(con, listener);

	// Materialized and sync streaming queries run normally with a default installed
	auto materialized = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(materialized, 0, {42}));
	auto sync_stream = con.SendQuery("SELECT i FROM range(100) t(i)", QueryParameters(true));
	REQUIRE(!sync_stream->HasError());
	DrainWatchdog watchdog(con);
	idx_t sync_rows = 0;
	while (auto chunk = sync_stream->Fetch()) {
		sync_rows += chunk->size();
	}
	REQUIRE(sync_rows == 100);

	// The same default still drives a later async query on this connection
	auto stream = ExecuteAsync(con, "SELECT i FROM range(1000) t(i)");
	Deadline deadline;
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async execution mode applies to the final statement of a multi-statement query", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	// Intermediates are materialized and sync, because the setting is ignored for them.
	// Only the final result is async and notifies
	NotifyListener listener;
	SetNotifier(con, listener);
	EnableAsync(con);
	auto result = con.SendQuery("SELECT 1; SELECT 2", QueryParameters(true));
	REQUIRE(!result->HasError());
	// Walk to the final result of the chain
	QueryResult *last = result.get();
	while (last->next) {
		last = last->next.get();
		REQUIRE(!last->HasError());
	}
	REQUIRE(last->GetResultType() == QueryResultType::STREAM_RESULT);
	auto &stream = last->Cast<StreamQueryResult>();
	Deadline deadline;
	idx_t row_count = 0;
	int64_t value = -1;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async mode is unaffected by an installed custom result collector", "[api][async_stream]") {
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

	// The async setting is ignored for materialized output, so the custom collector
	// hook still applies and produces the Arrow result exactly as in sync mode. The
	// SET goes through the context: with the hook installed even a SET's materialized
	// result becomes an Arrow result, which trips Connection::Query's assertion
	auto set_result = con.context->Query("SET streaming_execution_mode='async'",
	                                     QueryParameters(QueryResultOutputType::FORCE_MATERIALIZED));
	REQUIRE(!set_result->HasError());
	auto pending =
	    con.PendingQuery("SELECT i FROM range(10) t(i)", QueryParameters(QueryResultOutputType::FORCE_MATERIALIZED));
	REQUIRE(!pending->HasError());
	REQUIRE(pending->Execute()->GetResultType() == QueryResultType::ARROW_RESULT);

	// Async with streaming output bypasses the hook and keeps the async contract.
	// The mode is already set, so submit directly instead of through ExecuteAsync,
	// which would run another SET through Connection::Query
	auto stream_pending = con.PendingQuery("SELECT i FROM range(10000) t(i)", QueryParameters(true));
	REQUIRE(!stream_pending->HasError());
	auto stream_result = stream_pending->Execute();
	REQUIRE(stream_result->GetResultType() == QueryResultType::STREAM_RESULT);
	auto stream = unique_ptr_cast<QueryResult, StreamQueryResult>(std::move(stream_result));
	Deadline deadline;
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async stream result still supports blocking materialization", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM range(500000) t(i)");
	DrainWatchdog watchdog(con);
	auto materialized = stream->Materialize();
	REQUIRE(!materialized->HasError());
	REQUIRE(materialized->RowCount() == 500000);
}

TEST_CASE("Blocking materialization of an async batched stream result", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	DrainWatchdog watchdog(con);
	auto materialized = stream->Materialize();
	REQUIRE(!materialized->HasError());
	REQUIRE(materialized->RowCount() == 500000);
	REQUIRE(materialized->GetValue(0, 0).GetValue<int64_t>() == 0);
	REQUIRE(materialized->GetValue(0, 499999).GetValue<int64_t>() == 499999);
}

TEST_CASE("Async stream result on a table scan without insertion-order preservation", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i FROM range(10000) t(i)"));

	// Without insertion-order preservation the plain streaming collector serves a table scan
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
	auto stream = ExecuteAsync(con, "SELECT i FROM t");
	Deadline deadline;
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async stream result survives tiny stream buffers", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	// streaming_buffer_size has no lower bound. An async buffer must still admit chunks
	for (auto size : {"'0B'", "'1B'"}) {
		REQUIRE_NO_FAIL(con.Query(string("SET streaming_buffer_size=") + size));
		auto stream = ExecuteAsync(con, "SELECT i FROM range(10000) t(i)");
		Deadline deadline;
		idx_t row_count = 0;
		while (true) {
			REQUIRE(!deadline.Passed());
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
		DrainWatchdog watchdog(con);
		idx_t sync_rows = 0;
		while (auto sync_chunk = sync_stream->Fetch()) {
			sync_rows += sync_chunk->size();
		}
		REQUIRE(sync_rows == 10000);
	}
}

TEST_CASE("Async batched stream result survives tiny stream buffers", "[api][async_stream]") {
	for (auto buffer_size : {"0B", "1B"}) {
		DuckDB db(nullptr);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(250000)"));
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("SET streaming_buffer_size='%s'", buffer_size)));

		auto stream = ExecuteAsync(con, "SELECT i FROM t");
		Deadline deadline;
		int64_t next_value = 0;
		while (true) {
			REQUIRE(!deadline.Passed());
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

		// Sync leg: with both capacities floored at one byte the buffer still admits chunks.
		// Before the floors this blocked every sink on an always-full empty buffer, an
		// unbounded busy loop. The watchdog interrupt turns that regression into an error.
		std::atomic<bool> sync_leg_done {false};
		std::thread watchdog([&]() {
			Deadline watchdog_deadline;
			while (!sync_leg_done.load() && !watchdog_deadline.Passed()) {
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

// Fanout plans: async over a shared-CTE plan fed by the broadcast exchange

TEST_CASE("Async stream result drains a streaming-fanout CTE plan", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE fanout AS SELECT range i, range % 512 g FROM range(200000)"));

	// Distinct aggregates are rewritten into a shared materialized CTE: one scan fans out to both consumers
	const string query = "SELECT COUNT(DISTINCT i), SUM(DISTINCT g) FROM fanout";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	NotifyListener listener;
	SetNotifier(con, listener);
	auto stream = ExecuteAsync(con, query);
	Deadline deadline;
	idx_t row_count = 0;
	int64_t count_distinct = -1;
	int64_t sum_distinct = -1;
	while (true) {
		REQUIRE(!deadline.Passed());
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

TEST_CASE("Async batched stream result drains a streaming-fanout CTE plan", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET streaming_buffer_size='100KB'"));

	// Two readers over one table-backed materialized CTE: the batch index survives the
	// exchange, so the order-preserving batched collector consumes fanout output directly
	const string query = "WITH c AS MATERIALIZED (SELECT i FROM t) SELECT (SELECT COUNT(*) FROM c) total, i FROM c";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	NotifyListener listener;
	SetNotifier(con, listener);
	auto stream = ExecuteAsync(con, query);
	// PIPELINE_DEPENDENT alone does not pin the collector: without a batch index this
	// plan would silently fall back to the order-preserving simple collector
	REQUIRE_NOTHROW(stream->GetBufferedData().Cast<BatchedBufferedData>());
	Deadline deadline;
	int64_t next_value = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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

// The buffer cap: buffered bytes never exceed max_streaming_buffer_size, except that a
// single chunk larger than the cap is admitted into an empty pool

TEST_CASE("Async batched stream result never exceeds the buffer cap", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='250000b'"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA enable_profiling='no_output'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl AS SELECT 'padding-padding-' || i AS s FROM range(200000) t(i)"));

	auto stream = ExecuteAsync(con, "SELECT * FROM tbl");
	Deadline deadline;
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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
	REQUIRE(row_count == 200000);
	REQUIRE(stream->GetBufferedData().Cast<BatchedBufferedData>().PeakBufferedBytes() <= 250000);

	// The peak surfaces as a query-level profiling metric, carrying the real value
	auto peak = stream->GetBufferedData().Cast<BatchedBufferedData>().PeakBufferedBytes();
	auto profile = QueryProfiler::Get(*con.context).ToJSON();
	auto key_pos = profile.find("\"peak_streaming_buffer_size\"");
	REQUIRE(key_pos != string::npos);
	auto colon_pos = profile.find(':', key_pos);
	REQUIRE(colon_pos != string::npos);
	auto reported = std::stoull(profile.substr(colon_pos + 1));
	REQUIRE(reported == peak);
	REQUIRE(reported > 0);
}

TEST_CASE("Sync batched stream result never exceeds the buffer cap", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='250000b'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl AS SELECT 'padding-padding-' || i AS s FROM range(200000) t(i)"));

	auto pending = con.PendingQuery("SELECT * FROM tbl", QueryParameters(true));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	auto &stream = result->Cast<StreamQueryResult>();
	DrainWatchdog watchdog(con);
	idx_t row_count = 0;
	while (auto chunk = stream.Fetch()) {
		row_count += chunk->size();
	}
	REQUIRE(row_count == 200000);
	REQUIRE(stream.GetBufferedData().Cast<BatchedBufferedData>().PeakBufferedBytes() <= 250000);
}

TEST_CASE("Async simple stream result never exceeds the buffer cap", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100000b'"));

	// range() has no batch indexes, so this plan uses the simple buffer
	auto stream = ExecuteAsync(con, "SELECT i FROM range(500000) t(i)");
	Deadline deadline;
	idx_t row_count = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
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
	REQUIRE(row_count == 500000);
	REQUIRE(stream->GetBufferedData().Cast<SimpleBufferedData>().PeakBufferedBytes() <= 100000);
}

TEST_CASE("Sync simple stream result never exceeds the buffer cap", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// Regression: the sync consumer must stop replenishing on a saturated buffer, not
	// only on an exactly-full one, or the size-aware block spins the replenish loop
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100000b'"));

	auto pending = con.PendingQuery("SELECT i FROM range(500000) t(i)", QueryParameters(true));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	auto &stream = result->Cast<StreamQueryResult>();
	DrainWatchdog watchdog(con);
	idx_t row_count = 0;
	while (auto chunk = stream.Fetch()) {
		row_count += chunk->size();
	}
	REQUIRE(row_count == 500000);
	REQUIRE(stream.GetBufferedData().Cast<SimpleBufferedData>().PeakBufferedBytes() <= 100000);
}

TEST_CASE("A chunk larger than the buffer cap is admitted alone", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));
	// Every row holds a 150KB string, so any chunk exceeds the cap at every vector size
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE big AS SELECT repeat('x', 150000) || i AS s FROM range(8) t(i)"));

	auto stream = ExecuteAsync(con, "SELECT * FROM big");
	Deadline deadline;
	idx_t row_count = 0;
	idx_t largest_chunk = 0;
	while (true) {
		REQUIRE(!deadline.Passed());
		unique_ptr<DataChunk> chunk;
		auto execution_result = stream->TryFetchChunk(chunk);
		if (execution_result == StreamExecutionResult::CHUNK_READY) {
			row_count += chunk->size();
			largest_chunk = MaxValue<idx_t>(largest_chunk, chunk->GetDataSize());
			continue;
		}
		if (execution_result == StreamExecutionResult::EXECUTION_FINISHED) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(row_count == 8);
	// The floor admits one oversized chunk, but never stacks a second one on top,
	// so the peak stays within the honest bound: the cap plus one chunk
	auto peak = stream->GetBufferedData().Cast<BatchedBufferedData>().PeakBufferedBytes();
	REQUIRE(peak >= 150000);
	REQUIRE(largest_chunk >= 150000);
	REQUIRE(peak <= 100000 + largest_chunk);
}

#else

TEST_CASE("The async setting refuses a threadless build", "[api][async_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto result = con.Query("SET streaming_execution_mode='async'");
	REQUIRE(result->HasError());
}

#endif
