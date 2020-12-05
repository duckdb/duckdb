#include "duckdb/execution/operator/helper/physical_sample.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"

namespace duckdb {
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalSample &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	auto limit = make_unique<PhysicalSample>(op.types, op.sample_size);
	limit->children.push_back(move(plan));
	return move(limit);
}

} // namespace duckdb
