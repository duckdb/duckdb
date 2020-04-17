#include "duckdb/execution/operator/scan/physical_table_function.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_table_function.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalTableFunction &op) {
	assert(op.children.size() == 0);

	return make_unique<PhysicalTableFunction>(op.types, op.function, move(op.bind_data), move(op.parameters));
}
