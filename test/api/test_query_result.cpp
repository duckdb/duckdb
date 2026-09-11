#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/arrow/arrow_query_result.hpp"
#include "duckdb/common/arrow/physical_arrow_collector.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_result_stream.hpp"
#include "result_wait_helpers.hpp"

#include <atomic>
#include <chrono>
#include <thread>

using namespace duckdb;

namespace {

//! Submit a query, leaving the retention undecided
unique_ptr<QueryResult> Submit(Connection &con, const string &query, QueryParameters parameters = {}) {
	auto handle = con.Submit(query, std::move(parameters));
	if (handle->HasError()) {
		FAIL(handle->GetError());
	}
	return handle;
}

idx_t DrainCursor(QueryResult &result) {
	idx_t rows = 0;
	while (auto chunk = result.Fetch()) {
		rows += chunk->size();
	}
	return rows;
}

//! Stands in for an out-of-tree streaming collector: it builds its own result object and keeps the
//! query open, the combination no in-tree collector has
class TestCollectorState : public GlobalSinkState {
public:
	TestCollectorState(ClientContext &context, const vector<LogicalType> &types)
	    : collection(make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types)),
	      client_properties(context.GetClientProperties()) {
		collection->InitializeAppend(append_state);
	}

	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
	ClientProperties client_properties;
};

class TestStreamingCollector : public PhysicalResultCollector {
public:
	TestStreamingCollector(PhysicalPlan &physical_plan, PreparedStatementData &data)
	    : PhysicalResultCollector(physical_plan, data) {
	}

public:
	bool IsStreaming() const override {
		return true;
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<TestCollectorState>(context, types);
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &gstate = input.global_state.Cast<TestCollectorState>();
		gstate.collection->Append(gstate.append_state, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) const override {
		auto &gstate = state.Cast<TestCollectorState>();
		return make_uniq<QueryResult>(statement_type, properties, names, std::move(gstate.collection),
		                              gstate.client_properties);
	}
};

ScopedConfigSetting UseTestStreamingCollector(ClientConfig &config) {
	return ScopedConfigSetting(
	    config,
	    [](ClientConfig &config) {
		    config.get_result_collector = [](ClientContext &context,
		                                     PreparedStatementData &data) -> unique_ptr<PhysicalOperator> {
			    return make_uniq<TestStreamingCollector>(*data.physical_plan, data);
		    };
	    },
	    [](ClientConfig &config) { config.get_result_collector = nullptr; });
}

ScopedConfigSetting UseArrowCollector(ClientConfig &config) {
	return ScopedConfigSetting(
	    config,
	    [](ClientConfig &config) {
		    config.get_result_collector = [](ClientContext &context,
		                                     PreparedStatementData &data) -> unique_ptr<PhysicalOperator> {
			    return PhysicalArrowCollector::Create(context, data, STANDARD_VECTOR_SIZE);
		    };
	    },
	    [](ClientConfig &config) { config.get_result_collector = nullptr; });
}

} // namespace

#ifndef DUCKDB_NO_THREADS

TEST_CASE("Query returns a completed retained handle", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto result = con.Query("SELECT i FROM range(2000) t(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 2000);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 0);
	REQUIRE(result->GetValue(0, 1999).GetValue<int64_t>() == 1999);
	REQUIRE(result->Collection().Count() == 2000);
	REQUIRE(!result->ToString().empty());
	// The cursor walks the collection the handle already holds
	REQUIRE(DrainCursor(*result) == 2000);
	// Retention was settled at submission, so no producer ever parked
	REQUIRE(result->GetBufferedData().Lifetime() == ResultLifetime::RETAINED);
	REQUIRE(result->GetBufferedData().PeakBufferedBytes() == 0);
}

TEST_CASE("A submitted query parks for the consumer's choice", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	std::atomic<idx_t> notifications {0};
	QueryParameters parameters;
	parameters.notify_callback = [&notifications]() {
		notifications++;
	};
	auto handle = Submit(con, "SELECT i FROM range(500000) t(i)", parameters);

	// The park is reported before its notification runs, so wait for both
	Deadline deadline;
	while (handle->Poll() != QueryResultState::READY || notifications.load() == 0) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	// READY is the engine waiting on the consumer: a producer is parked with its first chunk
	REQUIRE(handle->GetBufferedData().WaitsOnConsumer());
	REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::UNDECIDED);
	REQUIRE(handle->GetBufferedData().PeakBufferedBytes() == 0);
	// The park is one transition, however many producers park on it, and nothing else can ring
	// while the retention stays undecided
	REQUIRE(notifications.load() == 1);
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	REQUIRE(notifications.load() == 1);
}

TEST_CASE("ExecuteTask on a parked undecided handle reports READY and runs nothing", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));

	auto handle = Submit(con, "SELECT i FROM range(500000) t(i)");
	Deadline deadline;
	QueryResultState state;
	while ((state = handle->ExecuteTask()) != QueryResultState::READY) {
		REQUIRE(state == QueryResultState::NOT_READY);
		REQUIRE(!deadline.Passed());
	}
	auto &buffer = handle->GetBufferedData();
	REQUIRE(buffer.WaitsOnConsumer());
	REQUIRE(buffer.Lifetime() == ResultLifetime::UNDECIDED);
	// The engine waits for the consumer's choice: stepping again decides nothing and runs nothing
	REQUIRE(handle->ExecuteTask() == QueryResultState::READY);
	REQUIRE(handle->ExecuteTask() == QueryResultState::READY);
	REQUIRE(handle->Poll() == QueryResultState::READY);
	REQUIRE(buffer.Lifetime() == ResultLifetime::UNDECIDED);
	REQUIRE(buffer.PeakBufferedBytes() == 0);
	// The choice releases the park
	REQUIRE(handle->Collection().Count() == 500000);
}

TEST_CASE("Materialize returns at once and the workers complete the query", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));

	// The unordered plan uses the simple store, the table scan the batched one
	for (auto query : {"SELECT i FROM range(200000) t(i)", "SELECT i FROM t"}) {
		std::atomic<idx_t> notifications {0};
		QueryParameters parameters;
		parameters.notify_callback = [&notifications]() {
			notifications++;
		};
		auto handle = Submit(con, query, parameters);

		handle->Materialize();
		// Nothing else is asked of the consumer: the terminal notification announces completion
		Deadline deadline;
		while (notifications.load() == 0 || handle->Poll() != QueryResultState::FINISHED) {
			REQUIRE(!deadline.Passed());
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		}
		REQUIRE(handle->Collection().Count() == 200000);
		REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::RETAINED);
		REQUIRE(handle->GetBufferedData().PeakBufferedBytes() == 0);
		// Collecting finished the query, so the connection is free
		auto next = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(next, 0, {42}));
	}
}

TEST_CASE("Collecting a fresh submission takes the retained path", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));

	auto handle = Submit(con, "SELECT i FROM range(500000) t(i)");
	DrainWatchdog watchdog(con);
	auto &collection = handle->Collection();
	REQUIRE(collection.Count() == 500000);
	REQUIRE(handle->GetValue(0, 0).GetValue<int64_t>() == 0);
	REQUIRE(handle->GetValue(0, 499999).GetValue<int64_t>() == 499999);
	// Producers appended into the collection: nothing was ever staged in the streaming buffer
	REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::RETAINED);
	REQUIRE(handle->GetBufferedData().PeakBufferedBytes() == 0);
	// Collection is idempotent once retained
	REQUIRE(&handle->Collection() == &collection);
}

TEST_CASE("TakeCollection hands the collection over exactly once", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto handle = Submit(con, "SELECT i FROM range(1000) t(i)");
	DrainWatchdog watchdog(con);
	auto collection = handle->TakeCollection();
	REQUIRE(collection);
	REQUIRE(collection->Count() == 1000);

	REQUIRE_THROWS_AS(handle->TakeCollection(), InvalidInputException);
	REQUIRE_THROWS_AS(handle->Collection(), InvalidInputException);
	REQUIRE_THROWS_AS(handle->Fetch(), InvalidInputException);
	// The collection outlives the handle it came from
	handle.reset();
	REQUIRE(collection->Count() == 1000);
}

TEST_CASE("An execution error surfaces on every retained-side call", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	// The cast fails at a late row, long after the submission succeeded
	auto handle =
	    Submit(con, "SELECT (CASE WHEN i = 40000 THEN 'boom' ELSE i::VARCHAR END)::INT FROM range(50000) t(i)");
	REQUIRE(!handle->HasError());

	handle->Materialize();
	Deadline deadline;
	while (handle->Poll() != QueryResultState::EXECUTION_ERROR) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(handle->HasError());
	REQUIRE(StringUtil::Contains(handle->GetError(), "boom"));
	REQUIRE_THROWS_AS(handle->Collection(), InvalidInputException);
	// GetValue throws the query's own error, not an internal one
	bool threw_query_error = false;
	try {
		handle->GetValue(0, 0);
	} catch (const std::exception &ex) {
		threw_query_error = StringUtil::Contains(ErrorData(ex).Message(), "boom");
	}
	REQUIRE(threw_query_error);
	REQUIRE(handle->RowCount() == 0);

	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("Poll observes an interrupt on a materializing handle", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto handle = Submit(con, "SELECT i FROM range(100000000000) t(i) WHERE i % 10 = 0");
	handle->Materialize();
	con.Interrupt();

	Deadline deadline;
	QueryResultState state;
	while (!IsTerminal(state = handle->Poll())) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(state == QueryResultState::EXECUTION_ERROR);
	REQUIRE(StringUtil::Contains(handle->GetError(), "INTERRUPT"));

	con.context->ClearInterrupt();
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A statement that completes on return is retained and refuses a stream", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));

	for (auto query : {"INSERT INTO t VALUES (1), (2) RETURNING i", "CREATE TABLE ctas AS SELECT 42 AS i"}) {
		auto handle = Submit(con, query);
		REQUIRE(handle->GetStatementProperties().result_eagerness == ResultEagerness::FORCED);
		// The store is settled before execution starts, so no producer parks for a decision
		REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::RETAINED);
		REQUIRE_THROWS_AS(QueryResultStream(std::move(handle)), InvalidInputException);
	}
	// The refused streams released their queries, so the connection is free again
	auto inserted = con.Query("INSERT INTO t VALUES (1), (2) RETURNING i");
	REQUIRE(inserted->RowCount() == 2);
}

TEST_CASE("Multi-statement text chains completed results", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto result = con.Query("CREATE TABLE t AS SELECT 42 AS i; SELECT i FROM t; SELECT 84 AS i;");
	REQUIRE_NO_FAIL(*result);
	// Every statement of the chain ran to completion and kept its result
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(result->next);
	auto &last = *result->next;
	REQUIRE(CHECK_COLUMN(last, 0, {84}));
	REQUIRE(!last.next);
	REQUIRE(last.RowCount() == 1);

	// A submission takes a single statement, and so does a parameterized eager query
	auto handle = con.Submit("SELECT 1; SELECT 2;");
	REQUIRE(handle->HasError());
	REQUIRE_FAIL(con.Query("SELECT $1; SELECT $1;", 1));
	auto single = con.Query("SELECT $1::INT", 7);
	REQUIRE(CHECK_COLUMN(single, 0, {7}));
}

TEST_CASE("A prepared statement gets a fresh store on every submission", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto prepared = con.Prepare("SELECT i FROM range($1) t(i)");
	REQUIRE(!prepared->HasError());

	DrainWatchdog watchdog(con);
	auto first = prepared->Submit(1000);
	auto &first_buffer = first->GetBufferedData();
	REQUIRE(first->Collection().Count() == 1000);

	auto second = prepared->Submit(2000);
	REQUIRE(&second->GetBufferedData() != &first_buffer);
	REQUIRE(second->Collection().Count() == 2000);
	// The first result is still readable: it holds its own collection
	REQUIRE(first->Collection().Count() == 1000);
}

TEST_CASE("A custom collector hands out its own result object", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &config = ClientConfig::GetConfig(*con.context);
	DrainWatchdog watchdog(con);

	SECTION("arrow collector, from Query and from Submit") {
		auto setting = UseArrowCollector(config);
		auto queried = con.Query("SELECT i FROM range(3000) t(i)");
		REQUIRE(queried->GetResultType() == QueryResultType::ARROW_RESULT);
		REQUIRE(!queried->HasError());
		REQUIRE(!queried->Cast<ArrowQueryResult>().Arrays().empty());

		auto submitted = con.Submit("SELECT i FROM range(3000) t(i)");
		REQUIRE(submitted->GetResultType() == QueryResultType::ARROW_RESULT);
		REQUIRE(!submitted->HasError());
	}
	SECTION("a streaming collector keeps the query open until its result is dropped") {
		auto setting = UseTestStreamingCollector(config);
		auto result = con.Submit("SELECT i FROM range(3000) t(i)");
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 3000);
	}
	// The connection is usable once the collector is gone
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A handle destroyed without collecting releases the query", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE fanout AS SELECT range i FROM range(200000)"));

	SECTION("a plain submission") {
		auto handle = Submit(con, "SELECT i FROM range(1000000) t(i)");
		REQUIRE(con.context->transaction.HasActiveTransaction());
		handle.reset();
		REQUIRE(!con.context->transaction.HasActiveTransaction());
	}
	SECTION("a fan-out plan with parked producers") {
		// One scan feeds two consumers: dropping the handle must unwind the parked producers
		auto handle = Submit(con, "WITH c AS MATERIALIZED (SELECT i FROM fanout) "
		                          "SELECT t1.i FROM c t1 JOIN c t2 USING (i)");
		Deadline deadline;
		while (!handle->GetBufferedData().WaitsOnConsumer()) {
			REQUIRE(!deadline.Passed());
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		}
		handle.reset();
	}
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

#endif
