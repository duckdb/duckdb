#include "duckdb/execution/operator/persistent/physical_batch_copy_to_file.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyToFile &op) {
	auto plan = CreatePlan(*op.children[0]);
	bool preserve_insertion_order = PhysicalPlanGenerator::PreserveInsertionOrder(context, *plan);
	bool supports_batch_index = PhysicalPlanGenerator::UseBatchIndex(context, *plan);
	auto &fs = FileSystem::GetFileSystem(context);
	op.file_path = fs.ExpandPath(op.file_path);
	if (op.use_tmp_file) {
		auto path = StringUtil::GetFilePath(op.file_path);
		auto base = StringUtil::GetFileName(op.file_path);
		op.file_path = fs.JoinPath(path, "tmp_" + base);
	}
	if (op.per_thread_output || op.file_size_bytes.IsValid() || op.rotate || op.partition_output ||
	    !op.partition_columns.empty() || op.overwrite_mode != CopyOverwriteMode::COPY_ERROR_ON_CONFLICT) {
		// hive-partitioning/per-thread output does not care about insertion order, and does not support batch indexes
		preserve_insertion_order = false;
		supports_batch_index = false;
	}
	auto mode = CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
	if (op.function.execution_mode) {
		mode = op.function.execution_mode(preserve_insertion_order, supports_batch_index);
	}
	if (mode == CopyFunctionExecutionMode::BATCH_COPY_TO_FILE) {
		if (!supports_batch_index) {
			throw InternalException("BATCH_COPY_TO_FILE can only be used if batch indexes are supported");
		}
		// batched copy to file
		auto copy = make_uniq<PhysicalBatchCopyToFile>(op.types, op.function, std::move(op.bind_data),
		                                               op.estimated_cardinality);
		copy->file_path = op.file_path;
		copy->use_tmp_file = op.use_tmp_file;
		copy->children.push_back(std::move(plan));
		copy->return_type = op.return_type;
		return std::move(copy);
	}

	// COPY from select statement to file
	auto copy = make_uniq<PhysicalCopyToFile>(op.types, op.function, std::move(op.bind_data), op.estimated_cardinality);
	copy->file_path = op.file_path;
	copy->use_tmp_file = op.use_tmp_file;
	copy->overwrite_mode = op.overwrite_mode;
	copy->filename_pattern = op.filename_pattern;
	copy->file_extension = op.file_extension;
	copy->per_thread_output = op.per_thread_output;
	if (op.file_size_bytes.IsValid()) {
		copy->file_size_bytes = op.file_size_bytes;
	}
	copy->rotate = op.rotate;
	copy->return_type = op.return_type;
	copy->partition_output = op.partition_output;
	copy->partition_columns = op.partition_columns;
	copy->write_partition_columns = op.write_partition_columns;
	copy->names = op.names;
	copy->expected_types = op.expected_types;
	copy->parallel = mode == CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;

	copy->children.push_back(std::move(plan));
	return std::move(copy);
}

} // namespace duckdb
