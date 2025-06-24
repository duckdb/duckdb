#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_export.hpp"
#include "duckdb/planner/operator/logical_export.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalExport &op) {
	auto &export_op = Make<PhysicalExport>(op.types, op.function, std::move(op.copy_info), op.estimated_cardinality,
	                                       std::move(op.exported_tables));
	// Plan the underlying copy statements, if any.
	if (!op.children.empty()) {
		auto &plan = CreatePlan(*op.children[0]);
		export_op.children.push_back(plan);
	}
	return export_op;
}

} // namespace duckdb
