#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/common/tree_renderer.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ExplainStatement &stmt) {
	BoundStatement result;

	// bind the underlying statement
	auto plan = Bind(*stmt.stmt);
	// render the unoptimized logical plan, but only when it will be shown: a plain EXPLAIN in a multi-plan format.
	// (it is unused for EXPLAIN ANALYZE, and single-plan formats like FORMAT WEB render only the final plan)
	string logical_plan_unopt;
	if (stmt.explain_type != ExplainType::EXPLAIN_ANALYZE) {
		auto renderer = TreeRenderer::CreateRenderer(context, stmt.format);
		if (!renderer || !renderer->RendersSinglePlan()) {
			logical_plan_unopt = plan.plan->ToString(context, stmt.format);
		}
	}
	auto explain = make_uniq<LogicalExplain>(std::move(plan.plan), stmt.explain_type, stmt.format);
	explain->logical_plan_unopt = logical_plan_unopt;

	result.plan = std::move(explain);
	result.names = {"explain_key", "explain_value"};
	result.types = {LogicalType::VARCHAR, LogicalType::VARCHAR};

	auto &properties = GetStatementProperties();
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb
