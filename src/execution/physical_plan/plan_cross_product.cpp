#include "execution/operator/join/physical_cross_product.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_cross_product.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCrossProduct &op) {
	assert(op.children.size() == 2);

	auto left = CreatePlan(*op.children[0]);
	auto right = CreatePlan(*op.children[1]);
	return make_unique<PhysicalCrossProduct>(op, move(left), move(right));
}
