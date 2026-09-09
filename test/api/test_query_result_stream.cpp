#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/query_result_stream.hpp"
#include "result_wait_helpers.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

using namespace duckdb;

namespace {

//! The consumer's side of the notify callback: it counts notifications and lets a waiter block
//! until the next one arrives
class NotifyChannel {
public:
	std::function<void()> Callback() {
		return [this]() {
			std::unique_lock<std::mutex> guard(lock);
			count++;
			callback_threads.insert(std::this_thread::get_id());
			signal.notify_all();
		};
	}

	//! Wait for a notification that has not been consumed yet. False when the deadline passed
	bool Wait(idx_t &seen) {
		Deadline deadline;
		std::unique_lock<std::mutex> guard(lock);
		while (count == seen) {
			if (signal.wait_for(guard, std::chrono::milliseconds(50)) == std::cv_status::timeout && deadline.Passed()) {
				return false;
			}
		}
		seen = count;
		return true;
	}

	idx_t Count() {
		std::unique_lock<std::mutex> guard(lock);
		return count;
	}

	bool RanOn(std::thread::id thread) {
		std::unique_lock<std::mutex> guard(lock);
		return callback_threads.count(thread) > 0;
	}

private:
	std::mutex lock;
	std::condition_variable signal;
	idx_t count = 0;
	duckdb::set<std::thread::id> callback_threads;
};

unique_ptr<QueryResultStream> OpenNotifyingStream(Connection &con, const string &query, NotifyChannel &channel) {
	QueryParameters parameters;
	parameters.notify_callback = channel.Callback();
	auto handle = con.Submit(query, parameters);
	if (handle->HasError()) {
		FAIL(handle->GetError());
	}
	return make_uniq<QueryResultStream>(std::move(handle));
}

} // namespace

#ifndef DUCKDB_NO_THREADS

TEST_CASE("A stream drains in order and settles the retention on draining", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));

	// The unordered plan uses the simple store, the table scan the batched one
	for (auto query : {"SELECT i FROM range(200000) t(i)", "SELECT i FROM t"}) {
		auto stream = OpenStream(con, query);
		DrainWatchdog watchdog(con);
		REQUIRE(stream->GetBufferedData().Lifetime() == ResultLifetime::DRAINING);
		int64_t expected = 0;
		while (auto chunk = stream->Fetch()) {
			for (idx_t i = 0; i < chunk->size(); i++) {
				REQUIRE(chunk->GetValue(0, i).GetValue<int64_t>() == expected);
				expected++;
			}
		}
		REQUIRE(!stream->HasError());
		REQUIRE(expected == 200000);
		// The guarantee is the cap plus one chunk: an admission into an empty buffer may exceed it
		// by at most the admitted chunk
		REQUIRE(stream->GetBufferedData().PeakBufferedBytes() <= 100000 + 100000);
	}
}

TEST_CASE("The stream constructor refuses a handle it cannot drain", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("an error handle") {
		auto handle = con.Submit("SELECT * FROM no_such_table");
		REQUIRE(handle->HasError());
		REQUIRE_THROWS_AS(QueryResultStream(std::move(handle)), InvalidInputException);
	}
	SECTION("a handle whose retention is already retained") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)");
		handle->Materialize();
		REQUIRE_THROWS_AS(QueryResultStream(std::move(handle)), InvalidInputException);
	}
	SECTION("a statement that completes on return") {
		auto handle = con.Submit("CREATE TABLE refused AS SELECT 42 AS i");
		REQUIRE_THROWS_AS(QueryResultStream(std::move(handle)), InvalidInputException);
	}
	// A refused stream released the query it consumed
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A query that produces no rows yields an empty stream", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000)"));

	auto stream = OpenStream(con, "SELECT i FROM t WHERE i < 0");
	unique_ptr<DataChunk> chunk;
	Deadline deadline;
	QueryResultState state = QueryResultState::NOT_READY;
	while (!IsTerminal(state)) {
		state = stream->TryFetch(chunk);
		REQUIRE(!chunk);
		REQUIRE(!deadline.Passed());
	}
	REQUIRE(state == QueryResultState::FINISHED);
	// The terminal state keeps repeating
	REQUIRE(stream->TryFetch(chunk) == QueryResultState::FINISHED);
	REQUIRE(stream->Poll() == QueryResultState::FINISHED);
	REQUIRE(!stream->Fetch());
}

TEST_CASE("A consumer drains without participating, waiting on the callback", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(100000)"));

	for (auto query : {"SELECT i FROM range(100000) t(i)", "SELECT i FROM t"}) {
		NotifyChannel channel;
		auto stream = OpenNotifyingStream(con, query, channel);
		DrainWatchdog watchdog(con);

		idx_t seen = 0;
		idx_t rows = 0;
		QueryResultState state = QueryResultState::NOT_READY;
		while (!IsTerminal(state)) {
			unique_ptr<DataChunk> chunk;
			state = stream->TryFetch(chunk);
			if (state == QueryResultState::READY) {
				// READY always carries a chunk
				REQUIRE(chunk);
				rows += chunk->size();
				continue;
			}
			REQUIRE(!chunk);
			if (!IsTerminal(state)) {
				// Nothing observable: the next notification is the only thing that wakes us
				REQUIRE(channel.Wait(seen));
			}
		}
		REQUIRE(state == QueryResultState::FINISHED);
		REQUIRE(rows == 100000);
		auto next = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(next, 0, {42}));
	}
}

TEST_CASE("A single-threaded consumer drains the stream itself", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));

	auto stream = OpenStream(con, "SELECT i FROM range(100000) t(i)");
	DrainWatchdog watchdog(con);
	idx_t rows = 0;
	Deadline deadline;
	QueryResultState state = QueryResultState::NOT_READY;
	while (!IsTerminal(state)) {
		unique_ptr<DataChunk> chunk;
		state = stream->TryFetch(chunk);
		if (state == QueryResultState::READY) {
			rows += chunk->size();
			continue;
		}
		if (IsTerminal(state)) {
			break;
		}
		// No worker will do it for us: the consumer runs the tasks
		if (stream->ExecuteTask() == QueryResultState::BLOCKED) {
			stream->WaitForTask();
		}
		REQUIRE(!deadline.Passed());
	}
	REQUIRE(state == QueryResultState::FINISHED);
	REQUIRE(rows == 100000);
}

TEST_CASE("The terminal notification wakes a consumer after the last chunk", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(2000000)"));

	// Every row is produced up front and the scan then runs on without producing anything more, so
	// the wake that ends this drain can only be the terminal notification
	NotifyChannel channel;
	auto stream = OpenNotifyingStream(con, "SELECT i FROM t WHERE i < 10", channel);
	DrainWatchdog watchdog(con);

	idx_t seen = 0;
	idx_t rows = 0;
	QueryResultState state = QueryResultState::NOT_READY;
	while (!IsTerminal(state)) {
		unique_ptr<DataChunk> chunk;
		state = stream->TryFetch(chunk);
		if (state == QueryResultState::READY) {
			rows += chunk->size();
			continue;
		}
		if (!IsTerminal(state)) {
			REQUIRE(channel.Wait(seen));
		}
	}
	REQUIRE(state == QueryResultState::FINISHED);
	REQUIRE(rows == 10);
}

TEST_CASE("An interrupt wakes a waiting consumer", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));

	NotifyChannel channel;
	auto stream = OpenNotifyingStream(con, "SELECT i FROM range(100000000000) t(i) WHERE i % 1000000 = 0", channel);

	// The query yields a row only once every million, so a chunk notification is not what wakes us
	unique_ptr<DataChunk> chunk;
	stream->TryFetch(chunk);
	auto seen = channel.Count();
	con.InterruptAndNotify();
	REQUIRE(channel.Wait(seen));

	Deadline deadline;
	QueryResultState state;
	while (!IsTerminal(state = stream->TryFetch(chunk))) {
		REQUIRE(!deadline.Passed());
	}
	REQUIRE(state == QueryResultState::EXECUTION_ERROR);
	REQUIRE(StringUtil::Contains(stream->GetError(), "INTERRUPT"));

	con.context->ClearInterrupt();
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("Interrupt sets the flag without ringing the notify callback", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// No worker threads: nothing runs, so only the interrupt itself could ring the callback
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));

	NotifyChannel channel;
	auto stream = OpenNotifyingStream(con, "SELECT i FROM range(1000000) t(i)", channel);
	con.Interrupt();
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(channel.Count() == 0);

	unique_ptr<DataChunk> chunk;
	REQUIRE(stream->TryFetch(chunk) == QueryResultState::EXECUTION_ERROR);
	REQUIRE(StringUtil::Contains(stream->GetError(), "INTERRUPT"));

	con.context->ClearInterrupt();
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("InterruptAndNotify wakes a consumer waiting on an idle engine", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	// No worker threads: no task will reach an interrupt check, so only the interrupt itself can ring
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));

	NotifyChannel channel;
	auto stream = OpenNotifyingStream(con, "SELECT i FROM range(1000000) t(i)", channel);
	auto seen = channel.Count();
	std::thread interrupter([&con]() { con.InterruptAndNotify(); });
	REQUIRE(channel.Wait(seen));
	interrupter.join();

	// No task ran to reach an interrupt check: the poll itself observes the flag
	REQUIRE(stream->Poll() == QueryResultState::EXECUTION_ERROR);
	REQUIRE(StringUtil::Contains(stream->GetError(), "INTERRUPT"));
	REQUIRE(!stream->IsOpen());
	// The terminal state keeps repeating
	unique_ptr<DataChunk> chunk;
	REQUIRE(stream->TryFetch(chunk) == QueryResultState::EXECUTION_ERROR);

	con.context->ClearInterrupt();
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("An error after the first chunk surfaces at the consumer's next call", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	// A cap below one chunk: a chunk is always buffered when the failing row is produced
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='1b'"));
	const auto rows = to_string(8 * STANDARD_VECTOR_SIZE);
	const auto boom = to_string(4 * STANDARD_VECTOR_SIZE);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(" + rows + ")"));

	const string cast = "SELECT (CASE WHEN i = " + boom + " THEN 'boom' ELSE i::VARCHAR END)::INT ";
	for (auto source : {"FROM range(" + rows + ") t(i)", string("FROM t")}) {
		auto stream = OpenStream(con, cast + source);
		DrainWatchdog watchdog(con);
		// The submission itself succeeded: the failure is only found while producing
		REQUIRE(!stream->HasError());

		unique_ptr<DataChunk> chunk;
		Deadline deadline;
		QueryResultState state = QueryResultState::NOT_READY;
		while (!IsTerminal(state)) {
			state = stream->TryFetch(chunk);
			REQUIRE(!deadline.Passed());
		}
		REQUIRE(state == QueryResultState::EXECUTION_ERROR);
		REQUIRE(StringUtil::Contains(stream->GetError(), "boom"));
		// The buffer still held a chunk when the error landed; none of it is reported afterwards
		REQUIRE(!chunk);
		REQUIRE(stream->TryFetch(chunk) == QueryResultState::EXECUTION_ERROR);
		REQUIRE(!chunk);

		auto next = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(next, 0, {42}));
	}
}

TEST_CASE("Participating calls never run the callback on the consumer's thread", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));

	auto consumer = std::this_thread::get_id();
	SECTION("a blocking drain of a stream") {
		NotifyChannel channel;
		auto stream = OpenNotifyingStream(con, "SELECT i FROM range(100000) t(i)", channel);
		DrainWatchdog watchdog(con);
		while (auto chunk = stream->Fetch()) {
		}
		REQUIRE(!stream->HasError());
		REQUIRE(!channel.RanOn(consumer));
	}
	SECTION("collecting a retained result") {
		NotifyChannel channel;
		QueryParameters parameters;
		parameters.notify_callback = channel.Callback();
		auto handle = con.Submit("SELECT i FROM range(100000) t(i)", parameters);
		DrainWatchdog watchdog(con);
		REQUIRE(handle->Collection().Count() == 100000);
		REQUIRE(!channel.RanOn(consumer));
	}
	SECTION("the same callback does run for a consumer that does not participate") {
		NotifyChannel channel;
		auto stream = OpenNotifyingStream(con, "SELECT i FROM range(100000) t(i)", channel);
		DrainWatchdog watchdog(con);
		idx_t seen = 0;
		REQUIRE(channel.Wait(seen));
		REQUIRE(!channel.RanOn(consumer));
	}
}

TEST_CASE("Close is a barrier for the callback", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));

	// Producers are still running when Close lands: the one test that legitimately loops attempts
	for (idx_t attempt = 0; attempt < 20; attempt++) {
		std::atomic<bool> closed {false};
		std::atomic<bool> notified_after_close {false};
		QueryParameters parameters;
		parameters.notify_callback = [&closed, &notified_after_close]() {
			if (closed.load()) {
				notified_after_close = true;
			}
		};
		auto handle = con.Submit("SELECT i FROM range(1000000) t(i)", parameters);
		REQUIRE(!handle->HasError());
		QueryResultStream stream(std::move(handle));
		// Pop a chunk first, so producers are running against the cap when Close lands
		unique_ptr<DataChunk> chunk;
		Deadline deadline;
		while (stream.TryFetch(chunk) != QueryResultState::READY) {
			REQUIRE(!deadline.Passed());
		}

		stream.Close();
		closed = true;
		REQUIRE(!notified_after_close.load());
	}
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A mid-stream fetch failure invalidates the open transaction", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(20000)"));
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	auto stream = OpenStream(con, "SELECT (CASE WHEN i = 10000 THEN 'boom' ELSE i::VARCHAR END)::INT FROM t");
	DrainWatchdog watchdog(con);
	while (auto chunk = stream->Fetch()) {
	}
	REQUIRE(stream->HasError());

	// The failure invalidated the transaction, per the connection's invalidation policy
	auto next = con.Query("SELECT 42");
	REQUIRE(next->HasError());
	REQUIRE(StringUtil::Contains(next->GetError(), "aborted"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	auto after = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(after, 0, {42}));
}

TEST_CASE("A second query on the same connection gets a fresh notifier", "[api][query_result_stream]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));

	NotifyChannel first_channel;
	{
		auto first = OpenNotifyingStream(con, "SELECT i FROM range(100000) t(i)", first_channel);
		unique_ptr<DataChunk> chunk;
		first->TryFetch(chunk);
		first->Close();
	}

	NotifyChannel second_channel;
	auto second = OpenNotifyingStream(con, "SELECT i FROM range(100000) t(i)", second_channel);
	DrainWatchdog watchdog(con);
	idx_t seen = 0;
	// The first stream's Close silenced its own notifier, not the connection
	REQUIRE(second_channel.Wait(seen));

	idx_t rows = 0;
	while (auto chunk = second->Fetch()) {
		rows += chunk->size();
	}
	REQUIRE(rows == 100000);
}

#endif
