#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/statement/bound_execute_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundExecuteStatement &stmt) {
	// all set, execute
	return make_unique<LogicalExecute>(stmt.prepared);
}
