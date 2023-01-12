#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/file_system.hpp"

#include <algorithm>

namespace duckdb {

class CopyToFunctionGlobalState : public GlobalSinkState {
public:
	explicit CopyToFunctionGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), last_file_offset(0), global_state(std::move(global_state)) {
	}
	mutex lock;
	idx_t rows_copied;
	idx_t last_file_offset;
	unique_ptr<GlobalFunctionData> global_state;

	//! shared state for HivePartitionedColumnData
	shared_ptr<GlobalHivePartitionState> partition_state;
};

class CopyToFunctionLocalState : public LocalSinkState {
public:
	explicit CopyToFunctionLocalState(unique_ptr<LocalFunctionData> local_state) : local_state(std::move(local_state)), writer_offset(0) {
	}
	unique_ptr<GlobalFunctionData> global_state;
	unique_ptr<LocalFunctionData> local_state;

	//! Buffers the tuples in partitions before writing
	unique_ptr<HivePartitionedColumnData> part_buffer;
	unique_ptr<PartitionedColumnDataAppendState> part_buffer_append_state;

	idx_t writer_offset;
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
void MoveTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_path = tmp_file_path.substr(0, tmp_file_path.length() - 4);
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

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                        DataChunk &input) const {
	auto &g = (CopyToFunctionGlobalState &)gstate;
	auto &l = (CopyToFunctionLocalState &)lstate;

	if (partition_output) {
		l.part_buffer->Append(*l.part_buffer_append_state, input);
		return SinkResultType::NEED_MORE_INPUT;
	}

	{
		lock_guard<mutex> glock(g.lock);
		g.rows_copied += input.size();
	}
	function.copy_to_sink(context, *bind_data, per_thread_output ? *l.global_state : *g.global_state, *l.local_state,
	                      input);
	return SinkResultType::NEED_MORE_INPUT;
}

static void CreateDir(ClientContext& context, const string& dir_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.DirectoryExists(dir_path)) {
		fs.CreateDirectory(dir_path);
	}
}

static string CreateDirRecursive(ClientContext& context, const vector<idx_t>& cols, const vector<string>& names, const vector<Value>& values, const string& path) {
	CreateDir(context, path);
	string partition_path = path + "/";

	for(idx_t i = 0; i < cols.size(); i++) {
		auto partition_col_name = names[cols[i]];
		auto partition_value = values[i];
		string p_dir = partition_col_name + "=" + partition_value.ToString() + "/";
		partition_path += p_dir + "/";
		CreateDir(context, partition_path);
	}

	return partition_path;
}

void PhysicalCopyToFile::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &g = (CopyToFunctionGlobalState &)gstate;
	auto &l = (CopyToFunctionLocalState &)lstate;

	if (partition_output) {
		l.part_buffer->FlushAppendState(*l.part_buffer_append_state);
		auto& partitions = l.part_buffer->GetPartitions();
		auto partition_key_map = l.part_buffer->GetReverseMap();

		for(idx_t i = 0; i < partitions.size(); i++) {
			string hive_path = CreateDirRecursive(context.client, partition_columns, names, partition_key_map[i]->values, file_path);

			// Create a writer for the current file
			auto fun_data_global = function.copy_to_initialize_global(context.client, *bind_data, hive_path + "/data_" + to_string(l.writer_offset) + ".parquet");
			auto fun_data_local = function.copy_to_initialize_local(context, *bind_data);

			for(auto &chunk : partitions[i]->Chunks()) {
				function.copy_to_sink(context, *bind_data, *fun_data_global, *fun_data_local, chunk);
			}

			function.copy_to_combine(context, *bind_data, *fun_data_global, *fun_data_local);
			function.copy_to_finalize(context.client, *bind_data, *fun_data_global);
		}

		return;
	}

	if (function.copy_to_combine) {
		function.copy_to_combine(context, *bind_data, per_thread_output ? *l.global_state : *g.global_state,
		                         *l.local_state);

		if (partition_output) {

		} else if (per_thread_output) {
			function.copy_to_finalize(context.client, *bind_data, *l.global_state);
		}
	}
}

SinkFinalizeType PhysicalCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              GlobalSinkState &gstate_p) const {
	auto &gstate = (CopyToFunctionGlobalState &)gstate_p;
	if (per_thread_output || partition_output) {
		// already happened in combine
		return SinkFinalizeType::READY;
	}
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state);

		if (use_tmp_file) {
			D_ASSERT(!per_thread_output); // FIXME
			MoveTmpFile(context, file_path);
		}
	}
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	if (partition_output) {
		auto state = make_unique<CopyToFunctionLocalState>(nullptr);
		{
			auto &g = (CopyToFunctionGlobalState &)*sink_state;
			lock_guard<mutex> glock(g.lock);
			state->writer_offset = g.last_file_offset++;

			// TODO: types is incorrect: its what copy returns, instead of what goes in here
			state->part_buffer = make_unique<HivePartitionedColumnData>(context.client, expected_types, partition_columns, g.partition_state);
			state->part_buffer_append_state = make_unique<PartitionedColumnDataAppendState>();
			state->part_buffer->InitializeAppendState(*state->part_buffer_append_state);
		}
		return std::move(state);
	}
	auto res = make_unique<CopyToFunctionLocalState>(function.copy_to_initialize_local(context, *bind_data));
	if (per_thread_output) {
		idx_t this_file_offset;
		{
			auto &g = (CopyToFunctionGlobalState &)*sink_state;
			lock_guard<mutex> glock(g.lock);
			this_file_offset = g.last_file_offset++;
		}
		auto &fs = FileSystem::GetFileSystem(context.client);
		string output_path = fs.JoinPath(file_path, StringUtil::Format("out_%llu", this_file_offset));
		if (fs.FileExists(output_path)) {
			throw IOException("%s exists", output_path);
		}
		res->global_state = function.copy_to_initialize_global(context.client, *bind_data, output_path);
	}
	return std::move(res);
}

unique_ptr<GlobalSinkState> PhysicalCopyToFile::GetGlobalSinkState(ClientContext &context) const {
	if (partition_output) {
		auto state = make_unique<CopyToFunctionGlobalState>(nullptr);
		state->partition_state = make_shared<GlobalHivePartitionState>();
		return state;
	} else if (per_thread_output) {
		auto &fs = FileSystem::GetFileSystem(context);

		if (fs.FileExists(file_path)) {
			throw IOException("%s exists", file_path);
		}
		if (!fs.DirectoryExists(file_path)) {
			fs.CreateDirectory(file_path);
		} else {
			idx_t n_files = 0;
			fs.ListFiles(file_path, [&n_files](const string &path, bool) { n_files++; });
			if (n_files > 0) {
				throw IOException("Directory %s is not empty", file_path);
			}
		}

		return make_unique<CopyToFunctionGlobalState>(nullptr);
	} else {
		auto &fs = FileSystem::GetFileSystem(context);
		auto found = file_path.find_last_of('/');
		auto dir = file_path.substr(0, found);

		if (!fs.DirectoryExists(dir)) {
			fs.CreateDirectory(dir);
		}
		return make_unique<CopyToFunctionGlobalState>(
		    function.copy_to_initialize_global(context, *bind_data, file_path));
	}
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class CopyToFileState : public GlobalSourceState {
public:
	CopyToFileState() : finished(false) {
	}

	bool finished;
};

unique_ptr<GlobalSourceState> PhysicalCopyToFile::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<CopyToFileState>();
}

void PhysicalCopyToFile::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                 LocalSourceState &lstate) const {
	auto &state = (CopyToFileState &)gstate;
	auto &g = (CopyToFunctionGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(g.rows_copied));
	state.finished = true;
}

} // namespace duckdb
