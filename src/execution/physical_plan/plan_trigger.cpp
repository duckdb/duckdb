#include "duckdb/execution/operator/persistent/physical_trigger.hpp"
#include "duckdb/execution/trigger_executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_trigger.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalTrigger &op) {
	D_ASSERT(op.children.size() == 1);
	D_ASSERT(!op.trigger_bodies.empty());
	D_ASSERT(op.trigger_bodies.size() == op.trigger_for_each.size());

	auto &child = CreatePlan(*op.children[0]);

	vector<TriggerInfo> triggers;
	triggers.reserve(op.trigger_bodies.size());
	for (idx_t i = 0; i < op.trigger_bodies.size(); i++) {
		triggers.push_back({std::move(op.trigger_bodies[i]), op.trigger_for_each[i]});
	}

	auto &trigger = Make<PhysicalTrigger>(std::move(triggers), 1);
	trigger.children.push_back(child);
	return trigger;
}

} // namespace duckdb
