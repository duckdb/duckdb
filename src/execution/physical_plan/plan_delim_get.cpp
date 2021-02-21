#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelimGet &op) {
	D_ASSERT(op.children.empty());

	// create a PhysicalChunkScan without an owned_collection, the collection will be added later
	auto chunk_scan =
	    make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::DELIM_SCAN, op.estimated_cardinality);
	return move(chunk_scan);
}

} // namespace duckdb
