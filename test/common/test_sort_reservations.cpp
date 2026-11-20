#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/temporary_memory_manager.hpp"

namespace duckdb {

static vector<BoundOrderByNode> ReservationTestOrders() {
	vector<BoundOrderByNode> orders;
	orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_LAST,
	                    make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));
	return orders;
}

TEST_CASE("Small sorts leave memory available to other operators", "[sort_reservations]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=4; SET memory_limit='256MiB'"));
	REQUIRE_NO_FAIL(
	    con.Query("SET temp_directory=" + Value(TestCreatePath("sort_initial_reservations")).ToSQLString()));
	con.BeginTransaction();
	auto &context = *con.context;
	Sort sort(context, ReservationTestOrders(), {LogicalType::BIGINT}, {});
	vector<unique_ptr<GlobalSinkState>> sinks;
	for (idx_t i = 0; i < 32; i++) {
		sinks.push_back(sort.GetGlobalSinkState(context));
	}
	auto other = TemporaryMemoryManager::Get(context.GetContext()).Register(context.GetContext());
	other->SetMinimumReservation(0);
	constexpr idx_t requested = 64ULL * 1024 * 1024;
	other->SetRemainingSizeAndUpdateReservation(context.GetContext(), requested);
	REQUIRE(other->GetReservation() == requested);

	InterruptState interrupt;
	for (auto &sink : sinks) {
		OperatorSinkFinalizeInput finalize {*sink, interrupt};
		REQUIRE(sort.Finalize(context, finalize) == SinkFinalizeType::NO_OUTPUT_POSSIBLE);
	}
	constexpr idx_t after_finalize = 200ULL * 1024 * 1024;
	other->SetRemainingSizeAndUpdateReservation(context.GetContext(), after_finalize);
	REQUIRE(other->GetReservation() == after_finalize);
}

} // namespace duckdb
