#include "execution/operator/schema/physical_create_table.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_create_table.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateTable &op) {
	auto create = make_unique<PhysicalCreateTable>(op, op.schema, move(op.info));
	if (op.children.size() > 0) {
		assert(op.children.size() == 1);
		auto plan = CreatePlan(*op.children[0]);
		create->children.push_back(move(plan));
	}
	return move(create);
}
