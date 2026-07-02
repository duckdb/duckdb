#include "duckdb/execution/operator/schema/physical_refresh_feature.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_refresh_feature.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalRefreshFeature &op) {
	D_ASSERT(op.children.size() == 1);
	auto &refresh = Make<PhysicalRefreshFeature>(std::move(op.feature_name), std::move(op.result_names),
	                                             std::move(op.result_types), op.estimated_cardinality);
	auto &plan = CreatePlan(*op.children[0]);
	refresh.children.push_back(plan);
	return refresh;
}

} // namespace duckdb
