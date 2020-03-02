#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/statement/bound_select_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundSelectStatement &stmt) {
	return CreatePlan(*stmt.node);
}
