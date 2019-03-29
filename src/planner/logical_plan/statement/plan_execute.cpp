#include "planner/expression/bound_parameter_expression.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_execute.hpp"
#include "planner/statement/bound_execute_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundExecuteStatement &stmt) {
	size_t param_idx = 1;
	for (auto val : stmt.values) {
		auto it = stmt.prep->value_map.find(param_idx);
		if (it == stmt.prep->value_map.end() || it->second == nullptr) {
			throw Exception("Could not find parameter with this index");
		}
		auto &target_value = it->second;
		if (target_value->type != val.type) {
			val = val.CastAs(target_value->type);
		}
		*target_value = val;
		param_idx++;
	}

	// all set, execute
	return make_unique<LogicalExecute>(stmt.prep);
}
