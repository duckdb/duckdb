#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/query_result_stream.hpp"

using namespace duckdb;

#ifndef DUCKDB_NO_THREADS

#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "result_wait_helpers.hpp"
#include "test_result_format.hpp"

namespace {

vector<int64_t> DrainRows(QueryResultStream<TestFormat> &stream, idx_t *unit_count = nullptr) {
	vector<int64_t> rows;
	while (auto unit = stream.Fetch()) {
		REQUIRE(unit->row_count > 0);
		for (auto &value : UnitValues(*unit, 0)) {
			rows.push_back(value.GetValue<int64_t>());
		}
		if (unit_count) {
			(*unit_count)++;
		}
	}
	REQUIRE(!stream.HasError());
	return rows;
}

void RequireSameMultiset(vector<int64_t> rows, idx_t expected_count) {
	REQUIRE(rows.size() == expected_count);
	std::sort(rows.begin(), rows.end());
	for (idx_t i = 0; i < rows.size(); i++) {
		REQUIRE(rows[i] == NumericCast<int64_t>(i));
	}
}

} // namespace

TEST_CASE("The format is observable right after Submit while the lifetime is still undecided",
          "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("a TestFormat") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)", make_shared_ptr<TestFormat>(4096));
		REQUIRE(!handle->HasError());
		REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::UNDECIDED);
		REQUIRE(StringUtil::Equals(handle->Format().Name(), TestFormat::NAME));
		auto &state = handle->FormatState<TestFormat>();
		REQUIRE(state.types.size() == 1);
		REQUIRE(state.names.size() == 1);
	}
	SECTION("the default chunk format") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)");
		REQUIRE(!handle->HasError());
		REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::UNDECIDED);
		REQUIRE(StringUtil::Equals(handle->Format().Name(), ChunkFormat::NAME));
		REQUIRE_NOTHROW(handle->FormatState<ChunkFormat>());
	}
}

TEST_CASE("A completed result reports the format it was submitted with", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	DrainWatchdog watchdog(con);

	auto format = ChunkFormat::BufferManaged();
	auto handle = con.Submit("SELECT i FROM range(5000) t(i)", format);
	REQUIRE(!handle->HasError());
	auto &state = handle->FormatState<ChunkFormat>();
	handle->Complete();
	REQUIRE_NO_FAIL(*handle);
	REQUIRE(&handle->Format() == format.get());
	REQUIRE(&handle->FormatState<ChunkFormat>() == &state);
	REQUIRE(handle->RowCount() == 5000);
}

TEST_CASE("A throwing InitGlobal surfaces from Submit", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto format = make_shared_ptr<TestFormat>(1024);
	format->throw_in_init_global = true;
	auto handle = con.Submit("SELECT i FROM range(1000) t(i)", std::move(format));
	REQUIRE(handle->HasError());
	REQUIRE(StringUtil::Contains(handle->GetError(), "TestFormat::InitGlobal"));

	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("InitGlobal runs on the submitting thread when a different thread drains", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto submitting_thread = std::this_thread::get_id();

	auto handle = con.Submit("SELECT i FROM range(20000) t(i)", make_shared_ptr<TestFormat>(4096));
	REQUIRE(!handle->HasError());
	// InitGlobal already ran, synchronously inside Submit, on this thread, before any drain starts
	REQUIRE(handle->FormatState<TestFormat>().init_global_thread == submitting_thread);

	DrainWatchdog watchdog(con);
	idx_t rows = 0;
	std::thread::id draining_thread;
	std::thread drainer([&]() {
		draining_thread = std::this_thread::get_id();
		QueryResultStream<TestFormat> stream(std::move(handle));
		while (auto unit = stream.Fetch()) {
			rows += unit->row_count;
		}
	});
	drainer.join();

	REQUIRE(rows == 20000);
	REQUIRE(draining_thread != submitting_thread);
}

TEST_CASE("A formatted stream drains an ordered plan in row order", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));

	SECTION("through the simple store") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(200000) t(i)", 4096);
		DrainWatchdog watchdog(con);
		QueryResultStream<TestFormat> stream(std::move(handle));
		REQUIRE(stream.GetBufferedData().Lifetime() == ResultLifetime::DRAINING);
		REQUIRE(stream.FormatState().types.size() == 1);
		REQUIRE_NOTHROW(stream.GetBufferedData().Cast<SimpleBufferedData>());
		RequireAscending(DrainRows(stream), 200000);
	}
	SECTION("through the batched store") {
		auto handle = SubmitFormatted(con, "SELECT i FROM t", 4096);
		DrainWatchdog watchdog(con);
		QueryResultStream<TestFormat> stream(std::move(handle));
		REQUIRE_NOTHROW(stream.GetBufferedData().Cast<BatchedBufferedData>());
		REQUIRE(stream.FormatState().ordering == ResultOrdering::BATCH_INDEX_ORDERED);
		RequireAscending(DrainRows(stream), 200000);
	}
}

TEST_CASE("A formatted stream of an order-free plan delivers the right multiset", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));
	REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));

	auto handle = SubmitFormatted(con, "SELECT i FROM t", 4096);
	DrainWatchdog watchdog(con);
	QueryResultStream<TestFormat> stream(std::move(handle));
	REQUIRE(stream.FormatState().ordering == ResultOrdering::UNORDERED);
	RequireSameMultiset(DrainRows(stream), 200000);
}

TEST_CASE("No unit spans two batch indexes", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));

	// A row cap well past one row group makes the batch boundary, not the cap, finish most units
	auto handle = SubmitFormatted(con, "SELECT i FROM t", 400000);
	DrainWatchdog watchdog(con);
	QueryResultStream<TestFormat> stream(std::move(handle));

	const int64_t group_size = NumericCast<int64_t>(DEFAULT_ROW_GROUP_SIZE);
	idx_t row_count = 0;
	idx_t units = 0;
	unordered_set<int64_t> groups_seen;
	while (auto unit = stream.Fetch()) {
		auto values = UnitValues(*unit, 0);
		REQUIRE(!values.empty());
		// A batch is one row group, so every row of a unit falls in the same row group
		const auto group = values.front().GetValue<int64_t>() / group_size;
		groups_seen.insert(group);
		for (auto &value : values) {
			REQUIRE(value.GetValue<int64_t>() / group_size == group);
		}
		row_count += unit->row_count;
		units++;
	}
	REQUIRE(!stream.HasError());
	REQUIRE(row_count == 500000);
	// One unit per row group: the row cap is never reached, so only the boundary finishes a unit
	const idx_t groups = (500000 + DEFAULT_ROW_GROUP_SIZE - 1) / DEFAULT_ROW_GROUP_SIZE;
	REQUIRE(groups > 1);
	REQUIRE(units == groups);
	REQUIRE(groups_seen.size() == groups);
	REQUIRE(stream.FormatState().partial_units == units);
}

TEST_CASE("Slicing at the cap finishes several units from one append", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	const idx_t row_count = 50000;
	const idx_t cap = 300;

	// A cap below one chunk, so a single append seals several units at once
	SECTION("drained") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(50000) t(i)", cap, true);
		DrainWatchdog watchdog(con);
		QueryResultStream<TestFormat> stream(std::move(handle));
		vector<int64_t> rows;
		idx_t units = 0;
		unique_ptr<TestPayload> previous;
		while (auto unit = stream.Fetch()) {
			if (previous) {
				REQUIRE(previous->row_count == cap);
			}
			for (auto &value : UnitValues(*unit, 0)) {
				rows.push_back(value.GetValue<int64_t>());
			}
			units++;
			previous = std::move(unit);
		}
		REQUIRE(!stream.HasError());
		RequireAscending(rows, row_count);
		REQUIRE(units > 1);
	}
	SECTION("retained") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(50000) t(i)", cap, true);
		DrainWatchdog watchdog(con);
		handle->Complete();
		auto &collection = handle->Collection<TestFormat>();
		REQUIRE(collection.size() > 1);
		vector<int64_t> rows;
		for (idx_t i = 0; i < collection.size(); i++) {
			if (i + 1 < collection.size()) {
				REQUIRE(collection[i]->row_count == cap);
			}
			for (auto &value : UnitValues(*collection[i], 0)) {
				rows.push_back(value.GetValue<int64_t>());
			}
		}
		RequireAscending(rows, row_count);
	}
}

TEST_CASE("Combine finishes the partial unit of every producer", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("a row cap no producer reaches") {
		// The simple store runs no NextBatch, so Combine is the only thing that can finish the unit
		auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 1000000);
		DrainWatchdog watchdog(con);
		QueryResultStream<TestFormat> stream(std::move(handle));
		idx_t units = 0;
		RequireAscending(DrainRows(stream, &units), 1000);
		REQUIRE(units == 1);
		REQUIRE(stream.FormatState().partial_units == 1);
	}
	SECTION("a producer whose partition holds no rows makes no unit") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(100000)"));
		auto handle = SubmitFormatted(con, "SELECT i FROM t WHERE i < 0", 1024);
		DrainWatchdog watchdog(con);
		QueryResultStream<TestFormat> stream(std::move(handle));
		REQUIRE(stream.Fetch() == nullptr);
		REQUIRE(!stream.HasError());
		REQUIRE(stream.FormatState().partial_units == 0);
	}
}

TEST_CASE("A throw from the format surfaces as the stream's error", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	enum class ThrowIn { APPEND, IS_FINISHED, FINISH };

	auto drain_with = [&](ThrowIn where) {
		auto format = make_shared_ptr<TestFormat>(1024);
		switch (where) {
		case ThrowIn::APPEND:
			format->throw_in_append = true;
			break;
		case ThrowIn::IS_FINISHED:
			format->throw_in_is_finished = true;
			break;
		case ThrowIn::FINISH:
			format->throw_in_finish = true;
			break;
		}
		auto handle = con.Submit("SELECT i FROM range(100000) t(i)", std::move(format));
		REQUIRE(!handle->HasError());
		DrainWatchdog watchdog(con);
		QueryResultStream<TestFormat> stream(std::move(handle));
		while (stream.Fetch()) {
		}
		REQUIRE(stream.HasError());
		const char *expected = where == ThrowIn::APPEND ? "TestFormat::AppendToUnit"
		                                                : (where == ThrowIn::IS_FINISHED ? "TestFormat::IsUnitFinished"
		                                                                                 : "TestFormat::FinishUnit");
		REQUIRE(StringUtil::Contains(stream.GetError(), expected));
	};

	SECTION("a throw in AppendToUnit") {
		drain_with(ThrowIn::APPEND);
	}
	SECTION("a throw in IsUnitFinished") {
		drain_with(ThrowIn::IS_FINISHED);
	}
	SECTION("a throw in FinishUnit") {
		drain_with(ThrowIn::FINISH);
	}
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A throw from the format on the retained path surfaces as the result's error", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto retain_with = [&](const char *expected, const std::function<void(TestFormat &)> &arm) {
		auto format = make_shared_ptr<TestFormat>(1024);
		arm(*format);
		auto handle = con.Submit("SELECT i FROM range(100000) t(i)", std::move(format));
		REQUIRE(!handle->HasError());
		DrainWatchdog watchdog(con);
		handle->Complete();
		REQUIRE(handle->HasError());
		REQUIRE(StringUtil::Contains(handle->GetError(), expected));
	};

	SECTION("a throw in AppendToUnit") {
		retain_with("TestFormat::AppendToUnit", [](TestFormat &format) { format.throw_in_append = true; });
	}
	SECTION("a throw in IsUnitFinished") {
		retain_with("TestFormat::IsUnitFinished", [](TestFormat &format) { format.throw_in_is_finished = true; });
	}
	SECTION("a throw in FinishUnit") {
		retain_with("TestFormat::FinishUnit", [](TestFormat &format) { format.throw_in_finish = true; });
	}
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A retained result in a format keeps its units in order", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));

	auto check_units = [](QueryResult &handle) {
		auto &collection = handle.Collection<TestFormat>();
		REQUIRE(handle.RowCount() == 200000);
		int64_t expected = 0;
		for (auto &unit : collection) {
			for (auto &value : UnitValues(*unit, 0)) {
				REQUIRE(value.GetValue<int64_t>() == expected);
				expected++;
			}
		}
		REQUIRE(expected == 200000);
		REQUIRE(handle.RowCount() == 200000);
		// The units are the format's, so the chunk accessors refuse this result
		REQUIRE_THROWS_AS(handle.Collection(), InvalidInputException);
		REQUIRE_THROWS_AS(handle.Fetch(), InvalidInputException);
	};

	SECTION("Materialize, then run it out to a terminal Poll") {
		auto handle = SubmitFormatted(con, "SELECT i FROM t", 4096);
		DrainWatchdog watchdog(con);
		handle->Materialize();
		QueryResultState state;
		Deadline deadline;
		while (!IsTerminal(state = handle->ExecuteTask())) {
			REQUIRE(!deadline.Passed());
		}
		REQUIRE(state == QueryResultState::FINISHED);
		REQUIRE(handle->Poll() == QueryResultState::FINISHED);
		check_units(*handle);
	}
	SECTION("Complete") {
		auto handle = SubmitFormatted(con, "SELECT i FROM t", 4096);
		DrainWatchdog watchdog(con);
		handle->Complete();
		check_units(*handle);
	}
}

TEST_CASE("Fetching from a retained format collection copies units out of an unchanged store",
          "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("every unit") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(10000) t(i)", 4096);
		DrainWatchdog watchdog(con);
		handle->Complete();

		auto &collection = handle->Collection<TestFormat>();
		const auto total_units = collection.size();
		REQUIRE(total_units > 1);

		vector<unique_ptr<TestPayload>> fetched;
		while (auto unit = handle->Fetch<TestFormat>()) {
			fetched.push_back(std::move(unit));
		}
		REQUIRE(fetched.size() == total_units);
		REQUIRE(!handle->Fetch<TestFormat>());

		REQUIRE(handle->RowCount() == 10000);
		REQUIRE(collection.size() == total_units);
		for (idx_t i = 0; i < total_units; i++) {
			REQUIRE(UnitValues(*fetched[i], 0) == UnitValues(*collection[i], 0));
		}

		// A fetched unit is a copy, so it outlives the result
		handle.reset();
		idx_t rows = 0;
		for (auto &unit : fetched) {
			rows += UnitValues(*unit, 0).size();
		}
		REQUIRE(rows == 10000);
	}
	SECTION("an empty store") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(0) t(i)", 4096);
		DrainWatchdog watchdog(con);
		handle->Complete();
		REQUIRE(!handle->Fetch<TestFormat>());
		REQUIRE(handle->Collection<TestFormat>().empty());
		REQUIRE(handle->RowCount() == 0);
	}
}

TEST_CASE("TakeCollection hands over the whole format store, fetched units included", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto handle = SubmitFormatted(con, "SELECT i FROM range(10000) t(i)", 4096);
	DrainWatchdog watchdog(con);
	handle->Complete();
	const auto total_units = handle->Collection<TestFormat>().size();
	REQUIRE(total_units > 1);
	REQUIRE(handle->Fetch<TestFormat>());

	auto collection = handle->TakeCollection<TestFormat>();
	REQUIRE(collection);
	REQUIRE(collection->size() == total_units);
	idx_t total_rows = 0;
	for (auto &payload : *collection) {
		total_rows += payload->row_count;
	}
	REQUIRE(total_rows == 10000);

	REQUIRE_THROWS_AS(handle->TakeCollection<TestFormat>(), InvalidInputException);
	REQUIRE_THROWS_AS(handle->Collection<TestFormat>(), InvalidInputException);
	REQUIRE_THROWS_AS(handle->Fetch<TestFormat>(), InvalidInputException);
	handle.reset();
	// The taken collection outlives the handle it came from
	idx_t total_rows_after_reset = 0;
	for (auto &payload : *collection) {
		total_rows_after_reset += payload->row_count;
	}
	REQUIRE(total_rows_after_reset == 10000);
}

TEST_CASE("A format given at submission reaches every completed result", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto format = make_shared_ptr<TestFormat>(4096);

	SECTION("a single statement") {
		auto result = con.context->Query("SELECT i FROM range(20000) t(i)", format);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == 20000);
	}
	SECTION("a statement that completes at submission") {
		auto result = con.context->Query("CREATE TABLE t AS SELECT range i FROM range(20000)", format);
		REQUIRE_NO_FAIL(*result);
		// The planner marks it FORCED, so the buffer settled the format before execution started
		REQUIRE(result->RowCount() == 1);
	}
	SECTION("every row-returning statement of a multi-statement query") {
		auto result = con.context->Query("SELECT 1 AS a; SELECT i FROM range(5000) t(i);", format);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == 1);
		REQUIRE(result->next);
		REQUIRE(result->next->RowCount() == 5000);
	}
}

TEST_CASE("The Connection Query and Submit format overloads reach the format", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	DrainWatchdog watchdog(con);

	SECTION("Query with a query string, given a shared_ptr<TestFormat>") {
		auto result = con.Query("SELECT i FROM range(1000) t(i)", make_shared_ptr<TestFormat>(4096));
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == 1000);
	}
	SECTION("Query with a statement, given a shared_ptr<TestFormat>") {
		auto statements = con.ExtractStatements("SELECT i FROM range(1000) t(i)");
		auto result = con.Query(std::move(statements[0]), make_shared_ptr<TestFormat>(4096));
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == 1000);
	}
	SECTION("Submit with a query string, given a shared_ptr<TestFormat>") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)", make_shared_ptr<TestFormat>(4096));
		REQUIRE(!handle->HasError());
		handle->Complete();
		REQUIRE(handle->RowCount() == 1000);
	}
	SECTION("Submit with a statement, given a shared_ptr<TestFormat>") {
		auto statements = con.ExtractStatements("SELECT i FROM range(1000) t(i)");
		auto handle = con.Submit(std::move(statements[0]), make_shared_ptr<TestFormat>(4096));
		REQUIRE(!handle->HasError());
		handle->Complete();
		REQUIRE(handle->RowCount() == 1000);
	}
	SECTION("Submit with a query string, given a plain shared_ptr<ResultFormat>") {
		shared_ptr<ResultFormat> format = make_shared_ptr<TestFormat>(4096);
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)", format);
		REQUIRE(!handle->HasError());
		handle->Complete();
		REQUIRE(handle->RowCount() == 1000);
	}
}

TEST_CASE("A stream and an accessor refuse a format that is not the settled one", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("a chunk stream on a formatted result") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 1024);
		REQUIRE_THROWS_AS(QueryResultStream<>(std::move(handle)), InvalidInputException);
	}
	SECTION("a formatted stream on a chunk result") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)");
		REQUIRE_THROWS_AS(QueryResultStream<TestFormat>(std::move(handle)), InvalidInputException);
	}
	SECTION("a formatted fetch on a chunk result") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)");
		DrainWatchdog watchdog(con);
		REQUIRE_THROWS_AS(handle->Fetch<TestFormat>(), InvalidInputException);
	}
	SECTION("a stream of another format that declares the same payload type") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 1024);
		REQUIRE_THROWS_AS(QueryResultStream<OtherTestFormat>(std::move(handle)), InvalidInputException);
	}
	SECTION("a fetch in another format that declares the same payload type") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 1024);
		DrainWatchdog watchdog(con);
		REQUIRE_THROWS_AS(handle->Fetch<OtherTestFormat>(), InvalidInputException);
		REQUIRE_THROWS_AS(handle->Collection<OtherTestFormat>(), InvalidInputException);
		REQUIRE_THROWS_AS(handle->FormatState<OtherTestFormat>(), InvalidInputException);
	}
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A fetched TestFormat payload outlives the result, connection, and database", "[api][query_result_format]") {
	unique_ptr<TestPayload> payload;
	{
		auto db = make_uniq<DuckDB>(nullptr);
		auto con = make_uniq<Connection>(*db);
		auto handle =
		    SubmitFormatted(*con, "SELECT i, 'payload beyond inlining length ' || i AS s FROM range(3000) t(i)", 4096);
		auto stream = make_uniq<QueryResultStream<TestFormat>>(std::move(handle));
		payload = stream->Fetch();
		REQUIRE(payload);
		REQUIRE(payload->row_count > 0);
		stream.reset();
		con.reset();
		db.reset();
	}
	// The payload's chunks are its own copies, so they read cleanly with nothing else left alive
	auto ints = UnitValues(*payload, 0);
	auto strings = UnitValues(*payload, 1);
	REQUIRE(ints.front().GetValue<int64_t>() == 0);
	auto last_row = ints.size() - 1;
	REQUIRE(strings.back().GetValue<string>() == "payload beyond inlining length " + to_string(last_row));
}

TEST_CASE("A payload fetched from a retained TestFormat result outlives the result, connection, and database",
          "[api][query_result_format]") {
	unique_ptr<TestPayload> payload;
	{
		auto db = make_uniq<DuckDB>(nullptr);
		auto con = make_uniq<Connection>(*db);
		auto handle =
		    SubmitFormatted(*con, "SELECT i, 'payload beyond inlining length ' || i AS s FROM range(3000) t(i)", 4096);
		handle->Complete();
		payload = handle->Fetch<TestFormat>();
		REQUIRE(payload);
		REQUIRE(payload->row_count > 0);
		handle.reset();
		con.reset();
		db.reset();
	}
	// Fetch copies the unit out of the collection, so it too reads cleanly with nothing else left alive
	auto ints = UnitValues(*payload, 0);
	auto strings = UnitValues(*payload, 1);
	REQUIRE(ints.front().GetValue<int64_t>() == 0);
	auto last_row = ints.size() - 1;
	REQUIRE(strings.back().GetValue<string>() == "payload beyond inlining length " + to_string(last_row));
}

TEST_CASE("TryFetch returns the same payload rows, in order, as Fetch", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));

	vector<int64_t> fetched_rows;
	{
		auto handle = SubmitFormatted(con, "SELECT i FROM t", 4096);
		DrainWatchdog watchdog(con);
		QueryResultStream<TestFormat> stream(std::move(handle));
		fetched_rows = DrainRows(stream);
	}

	vector<int64_t> polled_rows;
	{
		auto handle = SubmitFormatted(con, "SELECT i FROM t", 4096);
		DrainWatchdog watchdog(con);
		QueryResultStream<TestFormat> stream(std::move(handle));
		Deadline deadline;
		QueryResultState state = QueryResultState::NOT_READY;
		while (!IsTerminal(state)) {
			unique_ptr<TestPayload> payload;
			state = stream.TryFetch(payload);
			if (state == QueryResultState::READY) {
				for (auto &value : UnitValues(*payload, 0)) {
					polled_rows.push_back(value.GetValue<int64_t>());
				}
				continue;
			}
			if (IsTerminal(state)) {
				break;
			}
			// No worker will do it for us: the consumer runs the tasks
			if (stream.ExecuteTask() == QueryResultState::BLOCKED) {
				stream.WaitForTask();
			}
			REQUIRE(!deadline.Passed());
		}
		REQUIRE(state == QueryResultState::FINISHED);
		REQUIRE(!stream.HasError());
	}

	RequireAscending(fetched_rows, 200000);
	REQUIRE(fetched_rows == polled_rows);
}

TEST_CASE("RowCount throws after TakeCollection for a format", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 4096);
	DrainWatchdog watchdog(con);
	handle->Complete();
	auto collection = handle->TakeCollection<TestFormat>();
	REQUIRE(collection);
	// RowCount used to report 0 once the collection was taken, same as a result closed before it was
	// ever collected; it now throws so a taken result is distinguishable from an empty one
	REQUIRE_THROWS_AS(handle->RowCount(), InvalidInputException);
}

TEST_CASE("Fetch resumes where it left off across a Collection call, and stays null after exhaustion, for a format",
          "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto handle = SubmitFormatted(con, "SELECT i FROM range(10000) t(i)", 4096);
	DrainWatchdog watchdog(con);
	handle->Complete();

	auto first = handle->Fetch<TestFormat>();
	REQUIRE(first);
	auto first_rows = first->row_count;

	// Collection() does not disturb the Fetch cursor
	auto &collection = handle->Collection<TestFormat>();
	REQUIRE(collection.size() > 0);

	idx_t remaining_rows = 0;
	while (auto payload = handle->Fetch<TestFormat>()) {
		remaining_rows += payload->row_count;
	}
	REQUIRE(handle->RowCount() == first_rows + remaining_rows);
	REQUIRE(!handle->Fetch<TestFormat>());
	REQUIRE(!handle->Fetch<TestFormat>());
}

TEST_CASE("ToString, ToBox, Equals and FetchRaw on a retained TestFormat result pin today's behavior",
          "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 4096);
	DrainWatchdog watchdog(con);
	handle->Complete();
	REQUIRE(!handle->HasError());

	auto str = handle->ToString();
	REQUIRE(StringUtil::Contains(str, "Rows: 1000"));

	BoxRendererConfig config;
	ClientBoxRendererContext render_context(*con.context);
	auto box = handle->ToBox(render_context, config);
	REQUIRE(StringUtil::Contains(box, "Rows: 1000"));

	auto other = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 4096);
	other->Complete();
	REQUIRE_THROWS_AS(handle->Equals(*other), InvalidInputException);

	REQUIRE_THROWS_AS(handle->FetchRaw(), InvalidInputException);
}

TEST_CASE("Retained unordered and source-ordered TestFormat plans complete with every row",
          "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(400000)"));

	SECTION("unordered") {
		REQUIRE_NO_FAIL(con.Query("SET threads=4"));
		REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false"));
		auto handle = SubmitFormatted(con, "SELECT i FROM t", 4096);
		DrainWatchdog watchdog(con);
		REQUIRE(handle->FormatState<TestFormat>().ordering == ResultOrdering::UNORDERED);
		handle->Complete();
		REQUIRE(!handle->HasError());
		vector<int64_t> rows;
		for (auto &payload : handle->Collection<TestFormat>()) {
			for (auto &value : UnitValues(*payload, 0)) {
				rows.push_back(value.GetValue<int64_t>());
			}
		}
		RequireSameMultiset(rows, 400000);
	}
	SECTION("source ordered") {
		// A single-threaded scheduler cannot use a batch index, so preserve_insertion_order (the
		// default) falls back to SOURCE_ORDERED rather than BATCH_INDEX_ORDERED
		REQUIRE_NO_FAIL(con.Query("SET threads=1"));
		auto handle = SubmitFormatted(con, "SELECT i FROM t", 4096);
		DrainWatchdog watchdog(con);
		REQUIRE(handle->FormatState<TestFormat>().ordering == ResultOrdering::SOURCE_ORDERED);
		handle->Complete();
		REQUIRE(!handle->HasError());
		vector<int64_t> rows;
		for (auto &payload : handle->Collection<TestFormat>()) {
			for (auto &value : UnitValues(*payload, 0)) {
				rows.push_back(value.GetValue<int64_t>());
			}
		}
		RequireAscending(rows, 400000);
	}
}

TEST_CASE("Retained batch-ordered TestFormat with a cap below a row group", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(400000)"));
	// The actual row group count, not assumed: a parallel CTAS can leave row groups short of
	// DEFAULT_ROW_GROUP_SIZE, so a row-count-based ceiling division would not match
	auto groups_result = con.Query("SELECT count(DISTINCT row_group_id) FROM pragma_storage_info('t')");
	REQUIRE_NO_FAIL(*groups_result);
	const idx_t groups = groups_result->Collection().GetValue(0, 0).GetValue<idx_t>();
	REQUIRE(groups > 1);

	// Sliced at the cap, so a row group's rows do not divide evenly into whole units the way
	// whole-chunk concatenation can; well below one row group, so several payloads seal before the
	// batch boundary flushes the remainder
	auto handle = SubmitFormatted(con, "SELECT i FROM t", 5000, true);
	DrainWatchdog watchdog(con);
	REQUIRE(handle->FormatState<TestFormat>().ordering == ResultOrdering::BATCH_INDEX_ORDERED);
	handle->Complete();
	REQUIRE(!handle->HasError());

	auto &collection = handle->Collection<TestFormat>();
	REQUIRE(collection.size() > 1);
	REQUIRE(handle->RowCount() == 400000);

	int64_t expected = 0;
	for (auto &payload : collection) {
		for (auto &value : UnitValues(*payload, 0)) {
			REQUIRE(value.GetValue<int64_t>() == expected);
			expected++;
		}
	}
	REQUIRE(expected == 400000);

	// A batch boundary flushes one partial payload per row group, whether or not the cap was reached
	REQUIRE(handle->FormatState<TestFormat>().partial_units == groups);
}

TEST_CASE("Empty partitions under a parallel retained sink in a format", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(2000000)"));

	// Only the first row group yields rows, so most producers combine without ever having sunk a chunk
	for (auto preserve : {"true", "false"}) {
		REQUIRE_NO_FAIL(con.Query(string("SET preserve_insertion_order=") + preserve));
		auto handle = SubmitFormatted(con, "SELECT i FROM t WHERE i < 10", 4096);
		DrainWatchdog watchdog(con);
		handle->Complete();
		REQUIRE(!handle->HasError());
		REQUIRE(handle->RowCount() == 10);
	}
}

#endif
