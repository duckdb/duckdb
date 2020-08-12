#include "duckdb/execution/operator/scan/physical_table_function.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_table_function.hpp"

namespace duckdb {
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalTableFunction &op) {
	assert(op.children.size() == 0);

	auto tfd = (TableFunctionData *)op.bind_data.get();
	assert(tfd);
	// pass on bound column ids into the bind data so the function scan can see them
	tfd->column_ids = op.column_ids;
	return make_unique<PhysicalTableFunction>(op.types, op.function, move(op.bind_data), move(op.parameters));
}

} // namespace duckdb
