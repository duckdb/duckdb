#include "execution/operator/projection/physical_projection.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_projection.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalProjection &op) {
	assert(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);

	auto projection = make_unique<PhysicalProjection>(op, move(op.expressions));
	projection->children.push_back(move(plan));
	return move(projection);
}
