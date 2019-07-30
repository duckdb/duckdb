#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDelimGet &op) {
	assert(op.children.size() == 0);

	// create a PhysicalChunkScan without an owned_collection, the collection will be added later
	auto chunk_scan = make_unique<PhysicalChunkScan>(op.types, PhysicalOperatorType::DELIM_SCAN);
	return move(chunk_scan);
}
