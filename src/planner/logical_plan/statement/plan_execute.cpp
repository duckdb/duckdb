#include "planner/expression/bound_parameter_expression.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_execute.hpp"
#include "planner/statement/bound_execute_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundExecuteStatement &stmt) {
	size_t param_idx = 1;
	for (auto val : stmt.values) {
		auto it = stmt.prep->parameter_expression_map.find(param_idx);
		if (it == stmt.prep->parameter_expression_map.end() || it->second == nullptr) {
			throw Exception("Could not find parameter with this index");
		}
		BoundParameterExpression *param_expr = it->second;
		if (param_expr->return_type != val.type) {
			val = val.CastAs(param_expr->return_type);
		}
		param_expr->value = val;
		param_idx++;
	}

	// all set, execute
	return make_unique<LogicalExecute>(stmt.prep);
}
