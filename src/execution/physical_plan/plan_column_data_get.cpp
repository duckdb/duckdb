#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalColumnDataGet &op) {
	D_ASSERT(op.children.size() == 0);
	D_ASSERT(op.collection);

	// create a PhysicalChunkScan pointing towards the owned collection
	auto chunk_scan = make_uniq<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::COLUMN_DATA_SCAN,
	                                                    op.estimated_cardinality, std::move(op.collection));
	return std::move(chunk_scan);
}

} // namespace duckdb
