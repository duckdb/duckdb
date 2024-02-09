#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/storage_lock.hpp"

#include <algorithm>

namespace duckdb {

class CopyToFunctionGlobalState : public GlobalSinkState {
public:
	explicit CopyToFunctionGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), last_file_offset(0), global_state(std::move(global_state)) {
	}
	StorageLock lock;
	atomic<idx_t> rows_copied;
	atomic<idx_t> last_file_offset;
	unique_ptr<GlobalFunctionData> global_state;
	idx_t created_directories = 0;

	//! shared state for HivePartitionedColumnData
	shared_ptr<GlobalHivePartitionState> partition_state;
};

class CopyToFunctionLocalState : public LocalSinkState {
public:
	explicit CopyToFunctionLocalState(unique_ptr<LocalFunctionData> local_state)
	    : local_state(std::move(local_state)), writer_offset(0) {
	}
	unique_ptr<GlobalFunctionData> global_state;
	unique_ptr<LocalFunctionData> local_state;

	//! Buffers the tuples in partitions before writing
	unique_ptr<HivePartitionedColumnData> part_buffer;
	unique_ptr<PartitionedColumnDataAppendState> part_buffer_append_state;

	idx_t writer_offset;
};

unique_ptr<GlobalFunctionData> PhysicalCopyToFile::CreateFileState(ClientContext &context,
                                                                   GlobalSinkState &sink) const {
	auto &g = sink.Cast<CopyToFunctionGlobalState>();
	idx_t this_file_offset = g.last_file_offset++;
	auto &fs = FileSystem::GetFileSystem(context);
	string output_path(filename_pattern.CreateFilename(fs, file_path, file_extension, this_file_offset));
	if (fs.FileExists(output_path) && !overwrite_or_ignore) {
		throw IOException("%s exists! Enable OVERWRITE_OR_IGNORE option to force writing", output_path);
	}
	return function.copy_to_initialize_global(context, *bind_data, output_path);
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	if (partition_output) {
		auto &g = sink_state->Cast<CopyToFunctionGlobalState>();

		auto state = make_uniq<CopyToFunctionLocalState>(nullptr);
		state->writer_offset = g.last_file_offset++;
		state->part_buffer =
		    make_uniq<HivePartitionedColumnData>(context.client, expected_types, partition_columns, g.partition_state);
		state->part_buffer_append_state = make_uniq<PartitionedColumnDataAppendState>();
		state->part_buffer->InitializeAppendState(*state->part_buffer_append_state);
		return std::move(state);
	}
	auto res = make_uniq<CopyToFunctionLocalState>(function.copy_to_initialize_local(context, *bind_data));
	if (per_thread_output) {
		res->global_state = CreateFileState(context.client, *sink_state);
	}
	return std::move(res);
}

unique_ptr<GlobalSinkState> PhysicalCopyToFile::GetGlobalSinkState(ClientContext &context) const {

	if (partition_output || per_thread_output || file_size_bytes.IsValid()) {
		auto &fs = FileSystem::GetFileSystem(context);

		if (fs.FileExists(file_path) && !overwrite_or_ignore) {
			throw IOException("%s exists! Enable OVERWRITE_OR_IGNORE option to force writing", file_path);
		}
		if (!fs.DirectoryExists(file_path)) {
			fs.CreateDirectory(file_path);
		} else if (!overwrite_or_ignore) {
			idx_t n_files = 0;
			fs.ListFiles(file_path, [&n_files](const string &path, bool) { n_files++; });
			if (n_files > 0) {
				throw IOException("Directory %s is not empty! Enable OVERWRITE_OR_IGNORE option to force writing",
				                  file_path);
			}
		}

		auto state = make_uniq<CopyToFunctionGlobalState>(nullptr);
		if (!per_thread_output && file_size_bytes.IsValid()) {
			state->global_state = CreateFileState(context, *state);
		}

		if (partition_output) {
			state->partition_state = make_shared<GlobalHivePartitionState>();
		}

		return std::move(state);
	}

	return make_uniq<CopyToFunctionGlobalState>(function.copy_to_initialize_global(context, *bind_data, file_path));
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
void PhysicalCopyToFile::MoveTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);

	auto path = StringUtil::GetFilePath(tmp_file_path);
	auto base = StringUtil::GetFileName(tmp_file_path);

	auto prefix = base.find("tmp_");
	if (prefix == 0) {
		base = base.substr(4);
	}

	auto file_path = fs.JoinPath(path, base);
	if (fs.FileExists(file_path)) {
		fs.RemoveFile(file_path);
	}
	fs.MoveFile(tmp_file_path, file_path);
}

PhysicalCopyToFile::PhysicalCopyToFile(vector<LogicalType> types, CopyFunction function_p,
                                       unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::COPY_TO_FILE, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data)), parallel(false) {
}

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &g = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &l = input.local_state.Cast<CopyToFunctionLocalState>();

	if (partition_output) {
		l.part_buffer->Append(*l.part_buffer_append_state, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	g.rows_copied += chunk.size();

	if (per_thread_output) {
		auto &gstate = l.global_state;
		function.copy_to_sink(context, *bind_data, *gstate, *l.local_state, chunk);

		if (file_size_bytes.IsValid() && function.file_size_bytes(*gstate) > file_size_bytes.GetIndex()) {
			function.copy_to_finalize(context.client, *bind_data, *gstate);
			gstate = CreateFileState(context.client, *sink_state);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	if (!file_size_bytes.IsValid()) {
		function.copy_to_sink(context, *bind_data, *g.global_state, *l.local_state, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	// FILE_SIZE_BYTES is set, but threads write to the same file, synchronize using lock
	auto &gstate = g.global_state;
	auto lock = g.lock.GetExclusiveLock();
	if (function.file_size_bytes(*gstate) > file_size_bytes.GetIndex()) {
		auto owned_gstate = std::move(gstate);
		gstate = CreateFileState(context.client, *sink_state);
		lock.reset();
		function.copy_to_finalize(context.client, *bind_data, *owned_gstate);
	} else {
		lock.reset();
	}

	lock = g.lock.GetSharedLock();
	function.copy_to_sink(context, *bind_data, *gstate, *l.local_state, chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

static void CreateDir(const string &dir_path, FileSystem &fs) {
	if (!fs.DirectoryExists(dir_path)) {
		fs.CreateDirectory(dir_path);
	}
}

static void CreateDirectories(const vector<idx_t> &cols, const vector<string> &names, const vector<Value> &values,
                              string path, FileSystem &fs) {
	CreateDir(path, fs);

	for (idx_t i = 0; i < cols.size(); i++) {
		const auto &partition_col_name = names[cols[i]];
		const auto &partition_value = values[i];
		string p_dir = partition_col_name + "=" + partition_value.ToString();
		path = fs.JoinPath(path, p_dir);
		CreateDir(path, fs);
	}
}

static string GetDirectory(const vector<idx_t> &cols, const vector<string> &names, const vector<Value> &values,
                           string path, FileSystem &fs) {
	for (idx_t i = 0; i < cols.size(); i++) {
		const auto &partition_col_name = names[cols[i]];
		const auto &partition_value = values[i];
		string p_dir = partition_col_name + "=" + partition_value.ToString();
		path = fs.JoinPath(path, p_dir);
	}
	return path;
}

SinkCombineResultType PhysicalCopyToFile::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &g = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &l = input.local_state.Cast<CopyToFunctionLocalState>();

	if (partition_output) {
		auto &fs = FileSystem::GetFileSystem(context.client);
		l.part_buffer->FlushAppendState(*l.part_buffer_append_state);
		auto &partitions = l.part_buffer->GetPartitions();
		auto partition_key_map = l.part_buffer->GetReverseMap();

		string trimmed_path = file_path;
		StringUtil::RTrim(trimmed_path, fs.PathSeparator(trimmed_path));
		{
			// create directories
			auto lock = g.lock.GetExclusiveLock();
			lock_guard<mutex> global_lock_on_partition_state(g.partition_state->lock);
			const auto &global_partitions = g.partition_state->partitions;
			// global_partitions have partitions added only at the back, so it's fine to only traverse the last part

			for (idx_t i = g.created_directories; i < global_partitions.size(); i++) {
				CreateDirectories(partition_columns, names, global_partitions[i]->first.values, trimmed_path, fs);
			}
			g.created_directories = global_partitions.size();
		}

		for (idx_t i = 0; i < partitions.size(); i++) {
			string hive_path = GetDirectory(partition_columns, names, partition_key_map[i]->values, trimmed_path, fs);
			string full_path(filename_pattern.CreateFilename(fs, hive_path, file_extension, l.writer_offset));
			if (fs.FileExists(full_path) && !overwrite_or_ignore) {
				throw IOException(
				    "failed to create %s, file exists! Enable OVERWRITE_OR_IGNORE option to force writing", full_path);
			}
			// Create a writer for the current file
			auto fun_data_global = function.copy_to_initialize_global(context.client, *bind_data, full_path);
			auto fun_data_local = function.copy_to_initialize_local(context, *bind_data);

			for (auto &chunk : partitions[i]->Chunks()) {
				function.copy_to_sink(context, *bind_data, *fun_data_global, *fun_data_local, chunk);
			}

			function.copy_to_combine(context, *bind_data, *fun_data_global, *fun_data_local);
			function.copy_to_finalize(context.client, *bind_data, *fun_data_global);
		}
	} else if (function.copy_to_combine) {
		if (per_thread_output) {
			// For PER_THREAD_OUTPUT, we can combine/finalize immediately
			function.copy_to_combine(context, *bind_data, *l.global_state, *l.local_state);
			function.copy_to_finalize(context.client, *bind_data, *l.global_state);
		} else if (file_size_bytes.IsValid()) {
			// File in global state may change with FILE_SIZE_BYTES, need to grab lock
			auto lock = g.lock.GetSharedLock();
			function.copy_to_combine(context, *bind_data, *g.global_state, *l.local_state);
		} else {
			function.copy_to_combine(context, *bind_data, *g.global_state, *l.local_state);
		}
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFunctionGlobalState>();
	if (per_thread_output || partition_output) {
		// already happened in combine
		return SinkFinalizeType::READY;
	}
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state);

		if (use_tmp_file) {
			D_ASSERT(!per_thread_output);
			D_ASSERT(!partition_output);
			D_ASSERT(!file_size_bytes.IsValid());
			MoveTmpFile(context, file_path);
		}
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

SourceResultType PhysicalCopyToFile::GetData(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<CopyToFunctionGlobalState>();

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(g.rows_copied));

	return SourceResultType::FINISHED;
}

} // namespace duckdb
