#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/execution/operator/persistent/physical_batch_copy_to_file.hpp"
#include "duckdb/execution/operator/persistent/physical_fixed_batch_copy.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCopyToFile &op) {
	auto plan = CreatePlan(*op.children[0]);
	bool preserve_insertion_order = PhysicalPlanGenerator::PreserveInsertionOrder(context, *plan);
	bool supports_batch_index = PhysicalPlanGenerator::UseBatchIndex(context, *plan);
	auto &fs = FileSystem::GetFileSystem(context);
	op.file_path = fs.ExpandPath(op.file_path);
	if (op.use_tmp_file) {
		op.file_path += ".tmp";
	}
	if (op.per_thread_output || op.partition_output || !op.partition_columns.empty() || op.overwrite_or_ignore) {
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
		if (op.function.desired_batch_size) {
			auto copy = make_uniq<PhysicalFixedBatchCopy>(op.types, op.function, std::move(op.bind_data),
			                                              op.estimated_cardinality);
			copy->file_path = op.file_path;
			copy->use_tmp_file = op.use_tmp_file;
			copy->children.push_back(std::move(plan));
			return std::move(copy);
		} else {
			auto copy = make_uniq<PhysicalBatchCopyToFile>(op.types, op.function, std::move(op.bind_data),
			                                               op.estimated_cardinality);
			copy->file_path = op.file_path;
			copy->use_tmp_file = op.use_tmp_file;
			copy->children.push_back(std::move(plan));
			return std::move(copy);
		}
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
	copy->parallel = mode == CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;

	copy->children.push_back(std::move(plan));
	return std::move(copy);
}

} // namespace duckdb
