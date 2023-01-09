#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyToFile &op) {
	auto plan = CreatePlan(*op.children[0]);
	auto &fs = FileSystem::GetFileSystem(context);
	op.file_path = fs.ExpandPath(op.file_path, FileSystem::GetFileOpener(context));

	bool use_tmp_file = op.is_file_and_exists && op.use_tmp_file && !op.per_thread_output && !op.partition_output;
	if (use_tmp_file) {
		op.file_path += ".tmp";
	}
	// COPY from select statement to file
	auto copy = make_unique<PhysicalCopyToFile>(op.types, op.function, move(op.bind_data), op.estimated_cardinality);
	copy->file_path = op.file_path;
	copy->use_tmp_file = use_tmp_file;
	copy->per_thread_output = op.per_thread_output;
	copy->partition_output = op.partition_output;
	copy->partition_columns = op.partition_columns;
	copy->names = op.names;
	copy->expected_types = op.expected_types;
	if (op.function.parallel) {
		copy->parallel = op.function.parallel(context, *copy->bind_data);
	}

	copy->children.push_back(move(plan));
	return move(copy);
}

} // namespace duckdb
