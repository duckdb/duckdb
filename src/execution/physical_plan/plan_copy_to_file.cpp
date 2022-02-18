#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyToFile &op) {
	auto plan = CreatePlan(*op.children[0]);
	bool use_tmp_file = op.is_file_and_exists && op.use_tmp_file;
	if (use_tmp_file) {
		op.file_path += ".tmp";
	}
	// COPY from select statement to file
	auto copy = make_unique<PhysicalCopyToFile>(op.types, op.function, move(op.bind_data), op.estimated_cardinality);
	copy->file_path = op.file_path;
	copy->use_tmp_file = use_tmp_file;

	copy->children.push_back(move(plan));
	return move(copy);
}

} // namespace duckdb
