#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyToFile &op) {
	LogicalOperator* pop = (LogicalOperator*)op.children[0].get();
	auto plan = CreatePlan(*pop);
	auto &fs = FileSystem::GetFileSystem(context);
	op.file_path = fs.ExpandPath(op.file_path, FileSystem::GetFileOpener(context));
	if (op.use_tmp_file)
	{
		op.file_path += ".tmp";
	}
	// COPY from select statement to file
	auto copy = make_uniq<PhysicalCopyToFile>(op.types, op.function, std::move(op.bind_data), op.estimated_cardinality);
	copy->file_path = op.file_path;
	copy->use_tmp_file = op.use_tmp_file;
	copy->overwrite_or_ignore = op.overwrite_or_ignore;
	copy->filename_pattern = op.filename_pattern;
	copy->per_thread_output = op.per_thread_output;
	copy->partition_output = op.partition_output;
	copy->partition_columns = op.partition_columns;
	copy->names = op.names;
	copy->expected_types = op.expected_types;
	if (op.function.parallel)
	{
		copy->parallel = op.function.parallel(context, *copy->bind_data);
	}
	copy->children.push_back(std::move(plan));
	return std::move(copy);
}

} // namespace duckdb
