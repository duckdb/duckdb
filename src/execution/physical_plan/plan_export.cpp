#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_export.hpp"
#include "duckdb/planner/operator/logical_export.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalExport &op) {
	optional_ptr<PhysicalOperator> child;
	// Plan the underlying copy statements, if any.
	if (!op.children.empty()) {
		child = CreatePlan(*op.children[0]);
	}
	return Make<PhysicalExport>(child, op.types, op.function, std::move(op.copy_info), op.estimated_cardinality,
	                            std::move(op.exported_tables));
}

} // namespace duckdb
