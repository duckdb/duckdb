#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalDelimGet &op) {
	// Create a PhysicalChunkScan without an owned_collection.
	// We'll add the collection later.
	D_ASSERT(op.children.empty());
	return Make<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::DELIM_SCAN, op.estimated_cardinality, nullptr);
}

} // namespace duckdb
