#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_explain.hpp"
#include "duckdb/planner/statement/bound_explain_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundExplainStatement &stmt) {
	// first bind the root of the plan
	auto plan = CreatePlan(*stmt.bound_statement);
	// get the unoptimized logical plan, and create the explain statement
	auto logical_plan_unopt = plan->ToString();
	auto explain = make_unique<LogicalExplain>(move(plan));
	explain->logical_plan_unopt = logical_plan_unopt;
	return move(explain);
}
