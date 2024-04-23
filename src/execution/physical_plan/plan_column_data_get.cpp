#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalColumnDataGet &op) {
	D_ASSERT(op.children.size() == 0);

	if (op.collection) {
		// create a PhysicalChunkScan pointing towards the owned collection
		return make_uniq<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::COLUMN_DATA_SCAN,
		                                         op.estimated_cardinality, std::move(op.collection));
	} else {
		auto non_owning = make_uniq<PhysicalColumnDataScan>(op.types, PhysicalOperatorType::COLUMN_DATA_SCAN,
		                                                    op.estimated_cardinality);
		non_owning->collection = &op.to_scan;
		return std::move(non_owning);
	}
}

} // namespace duckdb
