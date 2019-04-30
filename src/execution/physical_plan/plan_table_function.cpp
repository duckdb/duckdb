#include "execution/operator/scan/physical_table_function.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_table_function.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalTableFunction &op) {
	assert(op.children.size() == 0);

	return make_unique<PhysicalTableFunction>(op, op.function, move(op.expressions));
}
