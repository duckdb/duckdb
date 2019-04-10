#include "execution/operator/helper/physical_execute.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_execute.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExecute &op) {
	assert(op.children.size() == 0);
	return make_unique<PhysicalExecute>(op.prep->plan.get());
}
