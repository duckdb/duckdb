#include "duckdb/execution/operator/persistent/physical_batch_copy_to_file.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalCopyToFile &op) {
	auto &plan = CreatePlan(*op.children[0]);
	bool preserve_insertion_order = PhysicalPlanGenerator::PreserveInsertionOrder(context, plan);
	bool supports_batch_index = PhysicalPlanGenerator::UseBatchIndex(context, plan);

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
		auto &copy =
		    Make<PhysicalBatchCopyToFile>(op.types, op.function, std::move(op.bind_data), op.estimated_cardinality);

		auto &cast_copy = copy.Cast<PhysicalBatchCopyToFile>();
		cast_copy.file_path = op.file_path;
		cast_copy.use_tmp_file = op.use_tmp_file;
		cast_copy.children.push_back(plan);
		cast_copy.return_type = op.return_type;
		cast_copy.write_empty_file = op.write_empty_file;
		return copy;
	}

	// COPY from select statement to file
	auto &copy = Make<PhysicalCopyToFile>(op.types, op.function, std::move(op.bind_data), op.estimated_cardinality);

	auto &cast_copy = copy.Cast<PhysicalCopyToFile>();
	cast_copy.file_path = op.file_path;
	cast_copy.use_tmp_file = op.use_tmp_file;
	cast_copy.overwrite_mode = op.overwrite_mode;
	cast_copy.filename_pattern = op.filename_pattern;
	cast_copy.file_extension = op.file_extension;
	cast_copy.per_thread_output = op.per_thread_output;

	if (op.file_size_bytes.IsValid()) {
		cast_copy.file_size_bytes = op.file_size_bytes;
	}

	cast_copy.rotate = op.rotate;
	cast_copy.return_type = op.return_type;
	cast_copy.partition_output = op.partition_output;
	cast_copy.partition_columns = op.partition_columns;
	cast_copy.write_partition_columns = op.write_partition_columns;
	cast_copy.names = op.names;
	cast_copy.expected_types = op.expected_types;
	cast_copy.parallel = mode == CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
	cast_copy.write_empty_file = op.write_empty_file;

	cast_copy.children.push_back(plan);
	return copy;
}

} // namespace duckdb
