#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ExplainStatement &stmt) {
	BoundStatement result;

	// bind the underlying statement
	auto plan = Bind(*stmt.stmt);
	// get the unoptimized logical plan, and create the explain statement
	auto logical_plan_unopt = plan.plan->ToString();
	auto explain = make_unique<LogicalExplain>(move(plan.plan), stmt.explain_type);
	explain->logical_plan_unopt = logical_plan_unopt;

	result.plan = move(explain);
	result.names = {"explain_key", "explain_value"};
	result.types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
