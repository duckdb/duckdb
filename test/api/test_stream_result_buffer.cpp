#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/query_parameters.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/stream_query_result.hpp"

#include <atomic>
#include <chrono>
#include <thread>

using namespace duckdb;

namespace {

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

unique_ptr<StreamQueryResult> ExecuteStreaming(Connection &con, const string &query) {
	auto pending = con.PendingQuery(query, QueryParameters(true));
	if (pending->HasError()) {
		FAIL(pending->GetError());
	}
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	return unique_ptr_cast<QueryResult, StreamQueryResult>(std::move(result));
}

//! Drive the non-blocking API until execution reaches a terminal state
StreamExecutionResult PollToTerminal(StreamQueryResult &stream) {
	Deadline deadline;
	while (true) {
		auto result = stream.ExecuteTask();
		if (result == StreamExecutionResult::EXECUTION_ERROR || result == StreamExecutionResult::EXECUTION_CANCELLED ||
		    result == StreamExecutionResult::EXECUTION_FINISHED) {
			return result;
		}
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
}

} // namespace

#ifndef DUCKDB_NO_THREADS

TEST_CASE("A blocking fetch on a batched stream observes an interrupt with chunks buffered", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));
	// A buffer large enough that chunks stay buffered when the interrupt arrives
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100MB'"));

	auto pending = con.PendingQuery("SELECT i FROM t", QueryParameters(true));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	auto &stream = result->Cast<StreamQueryResult>();
	// Wait until at least one chunk is observably buffered, then cancel before fetching
	Deadline deadline;
	while (stream.ExecuteTask() != StreamExecutionResult::CHUNK_READY) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	con.Interrupt();

	// The cancel must be observed even though chunks are buffered, not after the drain
	auto chunk = stream.Fetch();
	REQUIRE(!chunk);
	REQUIRE(stream.HasError());
	REQUIRE(StringUtil::Contains(stream.GetError(), "INTERRUPT"));
}

TEST_CASE("Blocking materialization of a simple stream result", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	auto stream = ExecuteStreaming(con, "SELECT i FROM range(500000) t(i)");
	DrainWatchdog watchdog(con);
	auto materialized = stream->Materialize();
	REQUIRE(!materialized->HasError());
	REQUIRE(materialized->RowCount() == 500000);
}

TEST_CASE("Blocking materialization of a batched stream result", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	auto stream = ExecuteStreaming(con, "SELECT i FROM t");
	DrainWatchdog watchdog(con);
	auto materialized = stream->Materialize();
	REQUIRE(!materialized->HasError());
	REQUIRE(materialized->RowCount() == 500000);
	REQUIRE(materialized->GetValue(0, 0).GetValue<int64_t>() == 0);
	REQUIRE(materialized->GetValue(0, 499999).GetValue<int64_t>() == 499999);
}

TEST_CASE("Materialize after a partial drain returns the remainder", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	auto stream = ExecuteStreaming(con, "SELECT i FROM range(500000) t(i)");
	DrainWatchdog watchdog(con);
	idx_t drained = 0;
	while (drained < 50000) {
		auto chunk = stream->Fetch();
		REQUIRE(chunk);
		drained += chunk->size();
	}
	// The fetch already chose the stream, so the remainder is copied out under the cap
	auto materialized = stream->Materialize();
	REQUIRE(!materialized->HasError());
	REQUIRE(materialized->RowCount() == 500000 - drained);
	REQUIRE(materialized->GetValue(0, 0).GetValue<int64_t>() == NumericCast<int64_t>(drained));
	REQUIRE(stream->GetBufferedData().Cast<SimpleBufferedData>().PeakBufferedBytes() <= 100000);
}

TEST_CASE("Materialize of an erroring stream surfaces the execution error", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	// The cast fails at a late row, after the stream has produced chunks
	auto stream = ExecuteStreaming(
	    con, "SELECT (CASE WHEN i = 400000 THEN 'boom' ELSE i::VARCHAR END)::INT FROM range(500000) t(i)");
	DrainWatchdog watchdog(con);
	auto materialized = stream->Materialize();
	REQUIRE(materialized->HasError());
	REQUIRE(StringUtil::Contains(materialized->GetError(), "boom"));
}

TEST_CASE("Materialize of an erroring batched stream surfaces the execution error", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	// Batched producers park under the cap before the late failing row
	auto stream = ExecuteStreaming(con, "SELECT (CASE WHEN i = 400000 THEN 'boom' ELSE i::VARCHAR END)::INT FROM t");
	DrainWatchdog watchdog(con);
	auto materialized = stream->Materialize();
	REQUIRE(materialized->HasError());
	REQUIRE(StringUtil::Contains(materialized->GetError(), "boom"));
}

TEST_CASE("A blocking drain of an erroring stream ends with the error", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	auto stream = ExecuteStreaming(
	    con, "SELECT (CASE WHEN i = 400000 THEN 'boom' ELSE i::VARCHAR END)::INT FROM range(500000) t(i)");
	DrainWatchdog watchdog(con);
	idx_t row_count = 0;
	while (auto chunk = stream->Fetch()) {
		row_count += chunk->size();
	}
	REQUIRE(stream->HasError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "boom"));
	// Rows before the failure may arrive, the failing chunk and anything after it never do
	REQUIRE(row_count < 400000);
}

TEST_CASE("A simple stream reports an execution error while a chunk is still buffered", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	// A cap below one chunk: the producer parks on every chunk, so one chunk is buffered when the error lands
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='1b'"));

	auto stream = ExecuteStreaming(con, "SELECT (CASE WHEN i = " + to_string(2 * STANDARD_VECTOR_SIZE) +
	                                        " THEN 'boom' ELSE i::VARCHAR END)::INT FROM range(" +
	                                        to_string(4 * STANDARD_VECTOR_SIZE) + ") t(i)");
	// The pop wakes the parked producer, whose next chunk fails on the worker thread
	REQUIRE(stream->Fetch());
	REQUIRE(PollToTerminal(*stream) == StreamExecutionResult::EXECUTION_ERROR);
	REQUIRE(stream->HasError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "boom"));
}

TEST_CASE("A batched stream reports an execution error while a chunk is still buffered", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(
	    con.Query("CREATE TABLE t AS SELECT range i FROM range(" + to_string(4 * STANDARD_VECTOR_SIZE) + ")"));
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='1b'"));

	auto stream = ExecuteStreaming(con, "SELECT (CASE WHEN i = " + to_string(2 * STANDARD_VECTOR_SIZE) +
	                                        " THEN 'boom' ELSE i::VARCHAR END)::INT FROM t");
	REQUIRE(stream->Fetch());
	REQUIRE(PollToTerminal(*stream) == StreamExecutionResult::EXECUTION_ERROR);
	REQUIRE(stream->HasError());
	REQUIRE(StringUtil::Contains(stream->GetError(), "boom"));
}

TEST_CASE("A blocking drain crosses a streaming-fanout CTE plan", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE fanout AS SELECT range i, range % 512 g FROM range(200000)"));

	// Distinct aggregates are rewritten into a shared materialized CTE: one scan fans out to both consumers
	const string query = "SELECT COUNT(DISTINCT i), SUM(DISTINCT g) FROM fanout";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	auto stream = ExecuteStreaming(con, query);
	DrainWatchdog watchdog(con);
	idx_t row_count = 0;
	int64_t count_distinct = -1;
	int64_t sum_distinct = -1;
	while (auto chunk = stream->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			count_distinct = chunk->GetValue(0, i).GetValue<int64_t>();
			sum_distinct = chunk->GetValue(1, i).GetValue<int64_t>();
		}
		row_count += chunk->size();
	}
	REQUIRE(!stream->HasError());
	REQUIRE(row_count == 1);
	REQUIRE(count_distinct == 200000);
	REQUIRE(sum_distinct == 130816);
}

TEST_CASE("A stream of nested types survives a tiny buffer", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	// A cap far below the chunk size: every producer parks, every copy is exercised
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='10KB'"));

	auto stream = ExecuteStreaming(
	    con, "SELECT [i, i + 1] l, {'a': i, 's': 'payload beyond inlining length ' || i} r FROM range(100000) t(i)");
	DrainWatchdog watchdog(con);
	idx_t row_count = 0;
	unique_ptr<DataChunk> last;
	while (auto chunk = stream->Fetch()) {
		row_count += chunk->size();
		last = std::move(chunk);
	}
	REQUIRE(!stream->HasError());
	REQUIRE(row_count == 100000);
	auto last_row = last->size() - 1;
	REQUIRE(last->GetValue(0, last_row).ToString() == "[99999, 100000]");
	REQUIRE(last->GetValue(1, last_row).ToString() == "{'a': 99999, 's': payload beyond inlining length 99999}");
}

TEST_CASE("A fanout plan streams under the cap instead of completing up front", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	// A materialized CTE consumed twice: the last pipeline is fed across a pipeline dependency
	const string query = "WITH c AS MATERIALIZED (SELECT range i FROM range(500000)) "
	                     "SELECT t1.i FROM c t1 JOIN c t2 USING (i)";
	REQUIRE(StringUtil::Contains(PhysicalPlanText(con, query), "PIPELINE_DEPENDENT"));

	auto stream = ExecuteStreaming(con, query);
	DrainWatchdog watchdog(con);
	idx_t row_count = 0;
	while (auto chunk = stream->Fetch()) {
		row_count += chunk->size();
	}
	REQUIRE(!stream->HasError());
	REQUIRE(row_count == 500000);
	// A completed-before-fetch execution would have buffered all ~4MB. Staying near the
	// cap proves producers parked, so the result was handed out mid-execution
	REQUIRE(stream->GetBufferedData().PeakBufferedBytes() <= 100000 + 100000);
}

TEST_CASE("A fetched stream chunk outlives result, connection, and database", "[api][stream_buffer]") {
	unique_ptr<DataChunk> chunk;
	{
		auto db = make_uniq<DuckDB>(nullptr);
		auto con = make_uniq<Connection>(*db);
		auto result =
		    ExecuteStreaming(*con, "SELECT i, 'payload beyond inlining length ' || i AS s FROM range(3000) t(i)");
		chunk = result->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() > 0);
		result.reset();
		con.reset();
		db.reset();
	}
	// The chunk's pins carry the database, so its vectors still read cleanly
	auto last_row = chunk->size() - 1;
	REQUIRE(chunk->GetValue(0, 0).GetValue<int64_t>() == 0);
	REQUIRE(chunk->GetValue(1, last_row).GetValue<string>() == "payload beyond inlining length " + to_string(last_row));
}

TEST_CASE("A batched stream result never exceeds the buffer cap", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='250000b'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl AS SELECT 'padding-padding-' || i AS s FROM range(200000) t(i)"));
	REQUIRE_NO_FAIL(con.Query("PRAGMA enable_profiling='no_output'"));

	auto pending = con.PendingQuery("SELECT * FROM tbl", QueryParameters(true));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	auto &stream = result->Cast<StreamQueryResult>();
	DrainWatchdog watchdog(con);
	idx_t row_count = 0;
	while (auto chunk = stream.Fetch()) {
		// The drain must observe exact insertion order through the park/deposit/move cycle
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto value = chunk->GetValue(0, i).ToString();
			auto expected = "padding-padding-" + to_string(row_count + i);
			if (value != expected) {
				FAIL(StringUtil::Format("Out-of-order row %llu: expected %s, got %s", row_count + i, expected, value));
			}
		}
		row_count += chunk->size();
	}
	REQUIRE(row_count == 200000);
	// The guarantee is the cap plus one chunk: an admission into an empty queue may
	// exceed the cap by at most the admitted chunk
	REQUIRE(stream.GetBufferedData().Cast<BatchedBufferedData>().PeakBufferedBytes() <= 250000 + 100000);

	// The peak surfaces as a query-level profiling metric, carrying the real value
	auto peak = stream.GetBufferedData().Cast<BatchedBufferedData>().PeakBufferedBytes();
	auto profile = QueryProfiler::Get(*con.context).ToJSON();
	auto key_pos = profile.find("\"peak_streaming_buffer_size\"");
	REQUIRE(key_pos != string::npos);
	auto colon_pos = profile.find(':', key_pos);
	REQUIRE(colon_pos != string::npos);
	auto reported = std::stoull(profile.substr(colon_pos + 1));
	REQUIRE(reported == peak);
	REQUIRE(reported > 0);
}

TEST_CASE("A batched stream survives a cap below one chunk", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(100000)"));
	// Every chunk exceeds the cap: each admission is the oversized-into-empty-queue case
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='1000b'"));

	auto pending = con.PendingQuery("SELECT i FROM t", QueryParameters(true));
	REQUIRE(!pending->HasError());
	auto result = pending->Execute();
	REQUIRE(result->GetResultType() == QueryResultType::STREAM_RESULT);
	auto &stream = result->Cast<StreamQueryResult>();
	DrainWatchdog watchdog(con);
	int64_t expected = 0;
	while (auto chunk = stream.Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			if (chunk->GetValue(0, i).GetValue<int64_t>() != expected) {
				FAIL(StringUtil::Format("Out-of-order row: expected %lld", expected));
			}
			expected++;
		}
	}
	REQUIRE(expected == 100000);
}

TEST_CASE("A simple stream result never exceeds the buffer cap", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	// The blocking consumer must stop replenishing on a saturated buffer, not only on
	// an exactly-full one, or the size-aware block spins the replenish loop
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

TEST_CASE("Execute returns before any chunk is buffered", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	for (auto query : {"SELECT i FROM range(500000) t(i)", "SELECT i FROM t"}) {
		auto stream = ExecuteStreaming(con, query);
		auto &buffered = stream->GetBufferedData();
		// The first producer parks with its chunk unconsumed until the consumer chooses
		REQUIRE(buffered.Lifetime() == ResultLifetime::UNDECIDED);
		REQUIRE(buffered.HasParkedProducer());
		REQUIRE(buffered.PeakBufferedBytes() == 0);
		DrainWatchdog watchdog(con);
		auto chunk = stream->Fetch();
		REQUIRE(chunk);
		REQUIRE(chunk->size() > 0);
		REQUIRE(buffered.Lifetime() == ResultLifetime::DRAINING);
		REQUIRE(buffered.PeakBufferedBytes() > 0);
	}
}

TEST_CASE("Materialize of a fresh stream stages nothing in the buffer", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='100KB'"));

	for (auto query : {"SELECT i FROM range(500000) t(i)", "SELECT i FROM t"}) {
		auto stream = ExecuteStreaming(con, query);
		DrainWatchdog watchdog(con);
		auto materialized = stream->Materialize();
		REQUIRE(!materialized->HasError());
		REQUIRE(materialized->RowCount() == 500000);
		REQUIRE(materialized->GetValue(0, 0).GetValue<int64_t>() == 0);
		REQUIRE(materialized->GetValue(0, 499999).GetValue<int64_t>() == 499999);
		// Producers appended into the collection directly: the streaming buffer never held a byte
		REQUIRE(stream->GetBufferedData().Lifetime() == ResultLifetime::RETAINED);
		REQUIRE(stream->GetBufferedData().PeakBufferedBytes() == 0);
	}
}

TEST_CASE("A zero-row batch-ordered stream materializes and drains", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));

	// No chunk ever reaches the sink, so the query finishes before the consumer decides
	{
		auto stream = ExecuteStreaming(con, "SELECT i FROM t WHERE i < 0");
		REQUIRE_NOTHROW(stream->GetBufferedData().Cast<BatchedBufferedData>());
		auto materialized = stream->Materialize();
		REQUIRE(!materialized->HasError());
		REQUIRE(materialized->RowCount() == 0);
	}
	{
		auto stream = ExecuteStreaming(con, "SELECT i FROM t WHERE i < 0");
		REQUIRE(!stream->Fetch());
		REQUIRE(!stream->HasError());
	}
}

TEST_CASE("Empty partitions under a parallel retained sink", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(2000000)"));

	// Only the first row group yields rows, so most producers combine without ever having sunk a chunk
	for (auto preserve : {"true", "false"}) {
		REQUIRE_NO_FAIL(con.Query(string("SET preserve_insertion_order=") + preserve));
		auto result = con.Query("SELECT i FROM t WHERE i < 10");
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 10);

		auto stream = ExecuteStreaming(con, "SELECT i FROM t WHERE i < 10");
		auto materialized = stream->Materialize();
		REQUIRE(!materialized->HasError());
		REQUIRE(materialized->RowCount() == 10);
	}
}

TEST_CASE("An interrupt during a retained materialize surfaces the interrupt", "[api][stream_buffer]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto stream = ExecuteStreaming(con, "SELECT i FROM range(100000000000) t(i) WHERE i % 10 = 0");
	std::thread interrupter([&con]() {
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
		con.Interrupt();
	});
	auto materialized = stream->Materialize();
	interrupter.join();
	REQUIRE(materialized->HasError());
	REQUIRE(StringUtil::Contains(materialized->GetError(), "INTERRUPT"));
}

#endif
