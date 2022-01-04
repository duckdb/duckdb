#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyToFile &op) {
	auto plan = CreatePlan(*op.children[0]);
	// COPY from select statement to file
	auto copy = make_unique<PhysicalCopyToFile>(op.types, op.function, move(op.bind_data), op.estimated_cardinality);

	copy->children.push_back(move(plan));
	return copy;
}

} // namespace duckdb
