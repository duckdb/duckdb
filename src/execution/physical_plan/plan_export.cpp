#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_export.hpp"
#include "duckdb/planner/operator/logical_export.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalExport &op) {
	auto export_node = make_uniq<PhysicalExport>(op.types, op.function, std::move(op.copy_info),
	                                             op.estimated_cardinality, std::move(op.exported_tables));
	// plan the underlying copy statements, if any
	if (!op.children.empty()) {
		auto plan = CreatePlan(*op.children[0]);
		export_node->children.push_back(std::move(plan));
	}
	return std::move(export_node);
}

} // namespace duckdb
