#include "execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/operator/logical_aggregate.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalAggregate &op) {
	assert(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
	if (op.groups.size() == 0) {
		// no groups
		// special case: aggregate entire columns together
		auto groupby = make_unique<PhysicalHashAggregate>(op.types, move(op.expressions));
		groupby->children.push_back(move(plan));
		return move(groupby);
	} else {
		// groups! create a GROUP BY aggregator
		auto groupby = make_unique<PhysicalHashAggregate>(op.types, move(op.expressions), move(op.groups));
		groupby->children.push_back(move(plan));
		return move(groupby);
	}
}
