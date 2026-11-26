#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/sorting/hashed_sort.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"
#include "test_helpers.hpp"

namespace duckdb {

TEST_CASE("Idle hash-group sorts leave memory available to other operators", "[sort][temporary_memory_manager]") {
	const auto variable_payload = GENERATE(false, true);
	idx_t row_count = 4096;
	bool bypass_sort = false;
	SECTION("Many small groups") {
	}
	SECTION("Single-key groups bypass sorting") {
		bypass_sort = true;
	}
	SECTION("Empty input") {
		row_count = 0;
	}

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1; SET memory_limit='64MiB'; SET operator_memory_limit='64MiB'"));
	REQUIRE_NO_FAIL(con.Query("SET debug_force_external=false"));
	REQUIRE_NO_FAIL(con.Query("SET temp_directory=" + Value(TestCreatePath("hashed_sort_reservation")).ToSQLString()));
	con.BeginTransaction();
	auto &client = *con.context;
	auto &context = client.GetContext();
	auto observer = TemporaryMemoryManager::Get(context).Register(context);
	observer->SetRemainingSizeAndUpdateReservation(context, 52ULL * 1024 * 1024);
	const auto available_before_sort = observer->GetReservation();
	REQUIRE(available_before_sort > 48ULL * 1024 * 1024);

	vector<unique_ptr<Expression>> partitions;
	partitions.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
	vector<BoundOrderByNode> orders;
	if (!bypass_sort) {
		orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST,
		                    make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 1), nullptr);
	}
	const vector<LogicalType> types {LogicalType::BIGINT, LogicalType::BIGINT,
	                                 variable_payload ? LogicalType::VARCHAR : LogicalType::BIGINT};
	HashedSort sort(client, partitions, orders, types, {}, row_count);
	auto global = sort.GetGlobalSinkState(client);
	ThreadContext thread(client);
	ExecutionContext execution(client, thread, nullptr);
	auto local = sort.GetLocalSinkState(execution);
	InterruptState interrupt;
	DataChunk chunk;
	chunk.Initialize(client, types);
	for (idx_t offset = 0; offset < row_count; offset += STANDARD_VECTOR_SIZE) {
		chunk.Reset();
		const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, row_count - offset);
		{
			auto keys = FlatVector::Writer<int64_t>(chunk.data[0], count);
			auto values = FlatVector::Writer<int64_t>(chunk.data[1], count);
			for (idx_t i = 0; i < count; i++) {
				keys.WriteValue(bypass_sort ? 0 : (offset + i) % 128);
				values.WriteValue(offset + i);
			}
		}
		if (variable_payload) {
			auto payload = FlatVector::Writer<string_t>(chunk.data[2], count);
			for (idx_t i = 0; i < count; i++) {
				payload.WriteValue(string_t("a variable-length payload longer than the inline string limit"));
			}
		} else {
			auto payload = FlatVector::Writer<int64_t>(chunk.data[2], count);
			for (idx_t i = 0; i < count; i++) {
				payload.WriteValue(offset + i);
			}
		}
		chunk.CheckCardinality(count);
		OperatorSinkInput input {*global, *local, interrupt};
		REQUIRE(sort.Sink(execution, chunk, input) == SinkResultType::NEED_MORE_INPUT);
	}
	OperatorSinkCombineInput combine {*global, *local, interrupt};
	REQUIRE(sort.Combine(execution, combine) == SinkCombineResultType::FINISHED);

	// Sinking and combining must not reserve memory for the deferred sorts.
	observer->UpdateReservation(context);
	REQUIRE(observer->GetReservation() == available_before_sort);

	OperatorSinkFinalizeInput finalize {*global, interrupt};
	const auto result = sort.Finalize(client, finalize);
	REQUIRE(result == (row_count ? SinkFinalizeType::READY : SinkFinalizeType::NO_OUTPUT_POSSIBLE));
	observer->UpdateReservation(context);
	if (bypass_sort || row_count == 0) {
		REQUIRE(observer->GetReservation() == available_before_sort);
	} else {
		// Small groups reserve their estimated size instead of one default minimum each.
		REQUIRE(observer->GetReservation() < available_before_sort);
		REQUIRE(observer->GetReservation() > available_before_sort * 9 / 10);
	}

	local.reset();
	global.reset();
	observer->UpdateReservation(context);
	REQUIRE(observer->GetReservation() == available_before_sort);
}

} // namespace duckdb
