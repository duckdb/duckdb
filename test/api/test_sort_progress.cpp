#include "duckdb.hpp"
#include "catch.hpp"
#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/common/sorting/full_sort.hpp"
#include "duckdb/common/sorting/hashed_sort.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;

TEST_CASE("Sort progress uses source units after cardinality changes", "[progress-bar][sort]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("BEGIN TRANSACTION")->HasError());
	auto &context = *con.context;
	vector<LogicalType> types {LogicalType::BIGINT};
	vector<BoundOrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                    make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
	Sort sort(context, orders, types, {0});
	ThreadContext thread(context);
	ExecutionContext execution(context, thread, nullptr);
	auto global = sort.GetGlobalSinkState(context);
	InterruptState interrupt;

	auto check = [&](double source_done, double source_total, double expected_done) {
		ProgressData source;
		source.done = source_done;
		source.total = source_total;
		auto progress = sort.GetSinkProgress(context, *global, source);
		CHECK(progress.IsValid());
		CHECK(progress.done == Approx(expected_done));
		CHECK(progress.total == Approx(source_total));
	};

	// Source progress can reach completion before the last input reaches the sink.
	check(1, 1, 0.5);
	check(90, 100, 45);
	check(0, 0, 0);

	DataChunk chunk;
	chunk.Initialize(context, types);
	{
		auto writer = FlatVector::Writer<int64_t>(chunk.data[0], 2);
		writer.WriteValue(2);
		writer.WriteValue(1);
	}
	chunk.SetChildCardinality(2);
	auto first = sort.GetLocalSinkState(execution);
	auto second = sort.GetLocalSinkState(execution);
	OperatorSinkInput first_input {*global, *first, interrupt};
	OperatorSinkInput second_input {*global, *second, interrupt};
	sort.Sink(execution, chunk, first_input);
	sort.Sink(execution, chunk, second_input);

	// Four accepted rows remain in unsorted local runs.
	check(1, 1, 0.5);
	check(100, 100, 50);
	check(1, 2, 0.5);
	OperatorSinkCombineInput first_combine {*global, *first, interrupt};
	sort.Combine(execution, first_combine);

	// Half of the accepted rows have been sorted, independently of source units.
	check(1, 1, 0.75);
	check(100, 100, 75);
	check(1, 2, 0.75);

	ProgressData invalid_source;
	invalid_source.done = 1.2;
	invalid_source.total = 1;
	REQUIRE_FALSE(sort.GetSinkProgress(context, *global, invalid_source).IsValid());
	invalid_source.SetInvalid();
	REQUIRE_FALSE(sort.GetSinkProgress(context, *global, invalid_source).IsValid());

	OperatorSinkCombineInput second_combine {*global, *second, interrupt};
	sort.Combine(execution, second_combine);
	check(1, 1, 1);
	check(4, 4, 4);
	check(100, 100, 100);
	check(1, 2, 1);
}

TEST_CASE("Empty sort source has no outstanding merge work", "[progress-bar][sort]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("BEGIN TRANSACTION")->HasError());
	auto &context = *con.context;
	vector<LogicalType> types {LogicalType::BIGINT};
	vector<BoundOrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                    make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
	Sort sort(context, orders, types, {0});
	auto sink = sort.GetGlobalSinkState(context);
	InterruptState interrupt;
	OperatorSinkFinalizeInput finalize {*sink, interrupt};
	ProgressData source_progress;
	source_progress.done = source_progress.total = 1;
	CHECK(sort.GetSinkProgress(context, *sink, source_progress).done == 0.5);
	REQUIRE(sort.Finalize(context, finalize) == SinkFinalizeType::NO_OUTPUT_POSSIBLE);
	CHECK(sort.GetSinkProgress(context, *sink, source_progress).done == 1);
	auto source = sort.GetGlobalSourceState(context, *sink);
	auto progress = sort.GetProgress(context, *source);
	CHECK(progress.IsValid());
	REQUIRE(progress.ProgressDone() == 1);
	progress.Normalize(100);
	REQUIRE(progress.done == 100);
}

TEST_CASE("Window sort wrappers preserve normalized progress units", "[progress-bar][sort]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_FALSE(con.Query("BEGIN TRANSACTION")->HasError());
	auto &context = *con.context;
	vector<LogicalType> types {LogicalType::BIGINT};
	vector<BoundOrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                    make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
	unique_ptr<SortStrategy> sort;
	bool hashed = false;
	SECTION("Full sort") {
		sort = make_uniq<FullSort>(context, orders, types);
	}
	SECTION("Partitioned sort") {
		vector<unique_ptr<Expression>> partitions;
		partitions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
		vector<unique_ptr<BaseStatistics>> statistics;
		sort = make_uniq<HashedSort>(context, partitions, orders, types, statistics, 1);
		hashed = true;
	}
	SECTION("Partitioned sort bypass") {
		vector<unique_ptr<Expression>> partitions;
		partitions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
		vector<unique_ptr<BaseStatistics>> statistics;
		vector<BoundOrderByNode> no_orders;
		sort = make_uniq<HashedSort>(context, partitions, no_orders, types, statistics, 1);
		hashed = true;
	}
	ThreadContext thread(context);
	ExecutionContext execution(context, thread, nullptr);
	auto global = sort->GetGlobalSinkState(context);
	auto local = sort->GetLocalSinkState(execution);
	InterruptState interrupt;
	ProgressData source;
	source.done = source.total = 1;
	auto empty_sink = sort->GetGlobalSinkState(context);
	CHECK(sort->GetSinkProgress(context, *empty_sink, source).done == 0.5);
	OperatorSinkFinalizeInput empty_finalize {*empty_sink, interrupt};
	REQUIRE(sort->Finalize(context, empty_finalize) == SinkFinalizeType::NO_OUTPUT_POSSIBLE);
	auto empty = sort->GetSinkProgress(context, *empty_sink, source);
	REQUIRE(empty.IsValid());
	CHECK(empty.done == 1);

	// A filter can reject early input and produce its first matching rows late in the scan.
	source.done = 90;
	source.total = 100;
	const auto before = sort->GetSinkProgress(context, *global, source);
	REQUIRE(before.IsValid());
	CHECK(before.done == 45);
	DataChunk chunk;
	chunk.Initialize(context, types);
	{
		auto writer = FlatVector::Writer<int64_t>(chunk.data[0], 2);
		writer.WriteValue(1);
		writer.WriteValue(1);
	}
	chunk.SetChildCardinality(2);
	OperatorSinkInput input {*global, *local, interrupt};
	sort->Sink(execution, chunk, input);
	source.done = 91;
	const auto after = sort->GetSinkProgress(context, *global, source);
	REQUIRE(after.IsValid());
	CHECK(after.done >= before.done);
	CHECK(after.done == Approx(45.5));
	source.done = source.total = 1;
	auto pending = sort->GetSinkProgress(context, *global, source);
	REQUIRE(pending.IsValid());
	REQUIRE(pending.done == Approx(0.5));
	OperatorSinkCombineInput combine {*global, *local, interrupt};
	sort->Combine(execution, combine);
	OperatorSinkFinalizeInput finalize {*global, interrupt};
	sort->Finalize(context, finalize);
	if (hashed) {
		auto global_source = sort->GetGlobalSourceState(context, *global);
		const auto &groups = sort->GetHashGroups(*global_source);
		for (idx_t group = 0; group < groups.size(); group++) {
			sort->SortColumnData(execution, group, finalize);
		}
	}
	auto completed = sort->GetSinkProgress(context, *global, source);
	CHECK(completed.IsValid());
	CHECK(completed.done == 1);
	REQUIRE(completed.total == 1);
	source.done = source.total = 100;
	completed = sort->GetSinkProgress(context, *global, source);
	CHECK(completed.IsValid());
	CHECK(completed.done == 100);
	REQUIRE(completed.total == 100);
}
