#include "planner/expression/bound_parameter_expression.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_execute.hpp"
#include "planner/statement/bound_execute_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundExecuteStatement &stmt) {
	// all set, execute
	return make_unique<LogicalExecute>(stmt.prep);
}
