#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalGet &op) {
	assert(op.children.size() == 0);

	if (!op.table) {
		return make_unique<PhysicalDummyScan>(op.types);
	} else {
		dependencies.insert(op.table);
		return make_unique<PhysicalTableScan>(op, *op.table, *op.table->storage, op.column_ids,move(op.expressions));
	}
}
