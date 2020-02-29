#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_prepare.hpp"
#include "duckdb/execution/operator/helper/physical_prepare.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalPrepare &op) {
	assert(op.children.size() == 1);

	// generate physical plan
	auto plan = CreatePlan(*op.children[0]);
	op.prepared->types = plan->types;
	op.prepared->plan = move(plan);
	op.prepared->dependencies = move(dependencies);

	return make_unique<PhysicalPrepare>(op.name, move(op.prepared));
}
