#include "duckdb/execution/operator/schema/physical_refresh_feature.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_refresh_feature.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalRefreshFeature &op) {
	return Make<PhysicalRefreshFeature>(std::move(op.feature_name), op.estimated_cardinality);
}

} // namespace duckdb
