#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/planner.hpp"

using namespace duckdb;

// The sorted aggregate rebind replaces the bound function with the buffering wrapper after all current
// CanThrow/IsVolatile consumers have run, so stale wrapper metadata has no SQL-observable surface today.
// These tests pin the wrapper's properties directly: the wrapper only buffers and sorts, so it must
// report the inner aggregate's error mode and stability, and nothing stricter.

namespace {

unique_ptr<LogicalOperator> PlanQuery(Connection &con, const string &query) {
	Parser parser(*con.context);
	parser.ParseQuery(query);
	REQUIRE(parser.statements.size() == 1);
	Planner planner(*con.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	return std::move(planner.plan);
}

optional_ptr<LogicalOperator> FindOperator(LogicalOperator &op, LogicalOperatorType type) {
	if (op.type == type) {
		return &op;
	}
	for (auto &child : op.children) {
		auto result = FindOperator(*child, type);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

//! Register a volatile copy of two-argument string_agg so stability propagation is observable
void RegisterVolatileStringAgg(Connection &con) {
	auto &context = *con.context;
	context.RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetSystemCatalog(context);
		auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(
		    context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), "string_agg"));
		auto function = *entry.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR, LogicalType::VARCHAR});
		function.SetName("test_volatile_string_agg");
		function.SetVolatile();
		CreateAggregateFunctionInfo info(function);
		catalog.CreateFunction(context, info);
	});
}

struct ExpectedProperties {
	FunctionErrors errors;
	FunctionStability stability;
};

} // namespace

TEST_CASE("Sorted aggregate rebind preserves the inner function properties", "[aggregate][sorted_aggregate]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(g INTEGER, s VARCHAR, j INTEGER)"));
	RegisterVolatileStringAgg(con);

	// string_agg is fallible, list is infallible, and the registered copy is fallible and volatile
	vector<ExpectedProperties> expected {{FunctionErrors::CAN_THROW_RUNTIME_ERROR, FunctionStability::CONSISTENT},
	                                     {FunctionErrors::CANNOT_ERROR, FunctionStability::CONSISTENT},
	                                     {FunctionErrors::CAN_THROW_RUNTIME_ERROR, FunctionStability::VOLATILE}};

	con.BeginTransaction();
	auto plan = PlanQuery(con, "SELECT string_agg(s, ',' ORDER BY j), list(s ORDER BY j), "
	                           "test_volatile_string_agg(s, ',' ORDER BY j) FROM t GROUP BY g");
	auto agg_op = FindOperator(*plan, LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY);
	REQUIRE(agg_op);
	auto &aggregate_op = agg_op->Cast<LogicalAggregate>();
	REQUIRE(aggregate_op.expressions.size() == expected.size());
	for (idx_t i = 0; i < expected.size(); i++) {
		auto &aggr = aggregate_op.expressions[i]->Cast<BoundAggregateExpression>();
		REQUIRE(aggr.GetOrderBys());
		REQUIRE(aggr.Function().GetErrorMode() == expected[i].errors);
		REQUIRE(aggr.Function().GetStability() == expected[i].stability);
		// the physical-planning rebind that replaces the function with the buffering wrapper
		FunctionBinder::BindSortedAggregate(*con.context, aggr, aggregate_op.groups, &aggregate_op.grouping_sets);
		REQUIRE(!aggr.GetOrderBys());
		REQUIRE(aggr.Function().GetErrorMode() == expected[i].errors);
		REQUIRE(aggr.Function().GetStability() == expected[i].stability);
	}
	con.Rollback();
}

TEST_CASE("Sorted window aggregate rebind preserves the inner function properties", "[aggregate][sorted_aggregate]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(g INTEGER, s VARCHAR, j INTEGER)"));

	// string_agg is fallible, list is infallible
	vector<pair<string, FunctionErrors>> cases {
	    {"string_agg(s, ',' ORDER BY j)", FunctionErrors::CAN_THROW_RUNTIME_ERROR},
	    {"list(s ORDER BY j)", FunctionErrors::CANNOT_ERROR}};

	con.BeginTransaction();
	for (auto &test_case : cases) {
		auto plan = PlanQuery(con, "SELECT " + test_case.first + " OVER (PARTITION BY g) FROM t");
		auto window_op = FindOperator(*plan, LogicalOperatorType::LOGICAL_WINDOW);
		REQUIRE(window_op);
		REQUIRE(window_op->expressions.size() == 1);
		auto &wexpr = window_op->expressions[0]->Cast<BoundWindowExpression>();
		REQUIRE(wexpr.AggregateFunction());
		REQUIRE(!wexpr.ArgOrders().empty());
		REQUIRE(wexpr.AggregateFunction()->GetErrorMode() == test_case.second);
		// the runtime rebind performed by the window constant aggregator
		FunctionBinder::BindSortedAggregate(*con.context, wexpr);
		REQUIRE(wexpr.ArgOrders().empty());
		REQUIRE(wexpr.AggregateFunction()->GetErrorMode() == test_case.second);
		REQUIRE(wexpr.AggregateFunction()->GetStability() == FunctionStability::CONSISTENT);
	}
	con.Rollback();
}
