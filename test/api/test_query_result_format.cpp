#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/query_result_stream.hpp"

using namespace duckdb;

#ifndef DUCKDB_NO_THREADS

#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "result_wait_helpers.hpp"
#include "test_result_format.hpp"

namespace {

//! Every row of every unit, in the order the stream delivered them
vector<int64_t> DrainRows(FormattedResultStream<TestFormat> &stream, idx_t *unit_count = nullptr) {
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

TEST_CASE("A formatted stream drains an ordered plan in row order", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));

	SECTION("through the simple store") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(200000) t(i)", 4096);
		DrainWatchdog watchdog(con);
		FormattedResultStream<TestFormat> stream(std::move(handle));
		REQUIRE(stream.GetBufferedData().Lifetime() == ResultLifetime::DRAINING);
		REQUIRE(stream.FormatState().types.size() == 1);
		REQUIRE_NOTHROW(stream.GetBufferedData().Cast<SimpleBufferedData>());
		RequireAscending(DrainRows(stream), 200000);
	}
	SECTION("through the batched store") {
		auto handle = SubmitFormatted(con, "SELECT i FROM t", 4096);
		DrainWatchdog watchdog(con);
		FormattedResultStream<TestFormat> stream(std::move(handle));
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
	FormattedResultStream<TestFormat> stream(std::move(handle));
	REQUIRE(stream.FormatState().ordering == ResultOrdering::UNORDERED);
	RequireSameMultiset(DrainRows(stream), 200000);
}

TEST_CASE("No unit spans two batch indexes", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(500000)"));

	// A row target well past one row group makes NextBatch, not IsFull, finish most units
	auto handle = SubmitFormatted(con, "SELECT i FROM t", 400000);
	DrainWatchdog watchdog(con);
	FormattedResultStream<TestFormat> stream(std::move(handle));

	const int64_t group_size = NumericCast<int64_t>(DEFAULT_ROW_GROUP_SIZE);
	idx_t row_count = 0;
	idx_t units = 0;
	unordered_set<idx_t> batches;
	while (auto unit = stream.Fetch()) {
		REQUIRE(unit->batch_index != DConstants::INVALID_INDEX);
		batches.insert(unit->batch_index);
		auto values = UnitValues(*unit, 0);
		REQUIRE(!values.empty());
		// A batch is one row group, so every row of a unit falls in the same row group
		const auto group = values.front().GetValue<int64_t>() / group_size;
		for (auto &value : values) {
			REQUIRE(value.GetValue<int64_t>() / group_size == group);
		}
		row_count += unit->row_count;
		units++;
	}
	REQUIRE(!stream.HasError());
	REQUIRE(row_count == 500000);
	// One unit per row group: the row target is never reached, so only the boundary finishes a unit
	const idx_t groups = (500000 + DEFAULT_ROW_GROUP_SIZE - 1) / DEFAULT_ROW_GROUP_SIZE;
	REQUIRE(groups > 1);
	REQUIRE(units == groups);
	REQUIRE(batches.size() == groups);
	REQUIRE(stream.FormatState().partial_units == units);
}

TEST_CASE("Combine finishes the partial unit of every producer", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("a row target no producer reaches") {
		// The simple store runs no NextBatch, so Combine is the only thing that can finish the unit
		auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 1000000);
		DrainWatchdog watchdog(con);
		FormattedResultStream<TestFormat> stream(std::move(handle));
		idx_t units = 0;
		RequireAscending(DrainRows(stream, &units), 1000);
		REQUIRE(units == 1);
		REQUIRE(stream.FormatState().partial_units == 1);
	}
	SECTION("a producer whose partition holds no rows makes no unit") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(100000)"));
		auto handle = SubmitFormatted(con, "SELECT i FROM t WHERE i < 0", 1024);
		DrainWatchdog watchdog(con);
		FormattedResultStream<TestFormat> stream(std::move(handle));
		REQUIRE(stream.Fetch() == nullptr);
		REQUIRE(!stream.HasError());
		REQUIRE(stream.FormatState().partial_units == 0);
	}
}

TEST_CASE("A throw from the format surfaces as the stream's error", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto drain_with = [&](bool in_append) {
		auto format = make_shared_ptr<TestFormat>(1024);
		if (in_append) {
			format->throw_in_append = true;
		} else {
			format->throw_in_finish = true;
		}
		auto handle = con.Submit("SELECT i FROM range(100000) t(i)");
		REQUIRE(!handle->HasError());
		handle->SetFormat(std::move(format));
		DrainWatchdog watchdog(con);
		FormattedResultStream<TestFormat> stream(std::move(handle));
		while (stream.Fetch()) {
		}
		REQUIRE(stream.HasError());
		REQUIRE(StringUtil::Contains(stream.GetError(), in_append ? "TestFormat::Append" : "TestFormat::Finish"));
	};

	SECTION("a throw in Append") {
		drain_with(true);
	}
	SECTION("a throw in Finish") {
		drain_with(false);
	}
	// The connection is usable afterwards
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A retained result in a format keeps its units in order", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));

	auto check_units = [](QueryResult &handle) {
		auto &collection = handle.Collection<TestFormat>();
		REQUIRE(collection.Count() == 200000);
		int64_t expected = 0;
		for (auto &unit : collection.Units()) {
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

TEST_CASE("Fetching from a retained format collection leaves its totals alone", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto handle = SubmitFormatted(con, "SELECT i FROM range(10000) t(i)", 4096);
	DrainWatchdog watchdog(con);
	handle->Complete();

	const auto total_units = handle->Collection<TestFormat>().UnitCount();
	REQUIRE(total_units > 1);
	REQUIRE(handle->RowCount() == 10000);

	auto first = handle->Fetch<TestFormat>();
	REQUIRE(first);
	REQUIRE(first->row_count == 4096);
	// Count and UnitCount stay what the collection was built with; only Units() shrinks
	REQUIRE(handle->RowCount() == 10000);
	REQUIRE(handle->Collection<TestFormat>().Count() == 10000);
	REQUIRE(handle->Collection<TestFormat>().UnitCount() == total_units);
	REQUIRE(handle->Collection<TestFormat>().Units().size() == total_units - 1);

	idx_t fetched = first->row_count;
	while (auto unit = handle->Fetch<TestFormat>()) {
		fetched += unit->row_count;
	}
	REQUIRE(fetched == 10000);
	REQUIRE(handle->Collection<TestFormat>().Units().empty());
	REQUIRE(handle->Collection<TestFormat>().Count() == 10000);
	REQUIRE(handle->RowCount() == 10000);
}

TEST_CASE("A format given at submission reaches every completed result", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	QueryParameters parameters;
	parameters.format = make_shared_ptr<TestFormat>(4096);

	SECTION("a single statement") {
		auto result = con.context->Query("SELECT i FROM range(20000) t(i)", parameters);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Collection<TestFormat>().Count() == 20000);
	}
	SECTION("a statement that completes at submission") {
		auto result = con.context->Query("CREATE TABLE t AS SELECT range i FROM range(20000)", parameters);
		REQUIRE_NO_FAIL(*result);
		// The planner marks it FORCED, so the buffer settled the format before execution started
		REQUIRE(result->Collection<TestFormat>().Count() == 1);
	}
	SECTION("every row-returning statement of a multi-statement query") {
		auto result = con.context->Query("SELECT 1 AS a; SELECT i FROM range(5000) t(i);", parameters);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->Collection<TestFormat>().Count() == 1);
		REQUIRE(result->next);
		REQUIRE(result->next->Collection<TestFormat>().Count() == 5000);
	}
}

TEST_CASE("SetFormat is refused once the result is decided", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto another = []() {
		return make_shared_ptr<TestFormat>(1024);
	};

	SECTION("after a fetch") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)");
		DrainWatchdog watchdog(con);
		REQUIRE(handle->Fetch());
		REQUIRE_THROWS_AS(handle->SetFormat(another()), InvalidInputException);
	}
	SECTION("never after a stream was opened: the stream consumed the handle SetFormat needs") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)");
		DrainWatchdog watchdog(con);
		QueryResultStream stream(std::move(handle));
		REQUIRE(!handle);
	}
	SECTION("after Materialize") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)");
		handle->Materialize();
		REQUIRE_THROWS_AS(handle->SetFormat(another()), InvalidInputException);
	}
	SECTION("on a Connection::Query result") {
		auto result = con.Query("SELECT i FROM range(1000) t(i)");
		REQUIRE_NO_FAIL(*result);
		REQUIRE_THROWS_AS(result->SetFormat(another()), InvalidInputException);
	}
	SECTION("but allowed after Poll and ExecuteTask, which settle nothing") {
		auto handle = con.Submit("SELECT i FROM range(200000) t(i)");
		DrainWatchdog watchdog(con);
		handle->Poll();
		handle->ExecuteTask();
		REQUIRE_NOTHROW(handle->SetFormat(another()));
		FormattedResultStream<TestFormat> stream(std::move(handle));
		RequireAscending(DrainRows(stream), 200000);
	}
}

TEST_CASE("A stream and an accessor refuse a format that is not the settled one", "[api][query_result_format]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("a chunk stream on a formatted result") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 1024);
		REQUIRE_THROWS_AS(QueryResultStream(std::move(handle)), InvalidInputException);
	}
	SECTION("a formatted stream on a chunk result") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)");
		REQUIRE_THROWS_AS(FormattedResultStream<TestFormat>(std::move(handle)), InvalidInputException);
	}
	SECTION("a formatted fetch on a chunk result") {
		auto handle = con.Submit("SELECT i FROM range(1000) t(i)");
		DrainWatchdog watchdog(con);
		REQUIRE_THROWS_AS(handle->Fetch<TestFormat>(), InvalidInputException);
	}
	SECTION("a stream of another format that shares the unit tag") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 1024);
		REQUIRE(TestFormat::TYPE == OtherTestFormat::TYPE);
		REQUIRE_THROWS_AS(FormattedResultStream<OtherTestFormat>(std::move(handle)), InvalidInputException);
	}
	SECTION("a fetch in another format that shares the unit tag") {
		auto handle = SubmitFormatted(con, "SELECT i FROM range(1000) t(i)", 1024);
		DrainWatchdog watchdog(con);
		REQUIRE(TestFormat::TYPE == OtherTestFormat::TYPE);
		REQUIRE_THROWS_AS(handle->Fetch<OtherTestFormat>(), InvalidInputException);
		REQUIRE_THROWS_AS(handle->Collection<OtherTestFormat>(), InvalidInputException);
		REQUIRE_THROWS_AS(handle->FormatState<OtherTestFormat>(), InvalidInputException);
	}
	// A refused stream released the query it consumed
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

#endif
