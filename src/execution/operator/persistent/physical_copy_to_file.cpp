#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include <algorithm>

namespace duckdb {

struct PartitionWriteInfo {
	unique_ptr<GlobalFunctionData> global_state;
	idx_t active_writes = 0;
};

struct VectorOfValuesHashFunction {
	uint64_t operator()(const vector<Value> &values) const {
		hash_t result = 0;
		for (auto &val : values) {
			result ^= val.Hash();
		}
		return result;
	}
};

struct VectorOfValuesEquality {
	bool operator()(const vector<Value> &a, const vector<Value> &b) const {
		if (a.size() != b.size()) {
			return false;
		}
		for (idx_t i = 0; i < a.size(); i++) {
			if (ValueOperations::DistinctFrom(a[i], b[i])) {
				return false;
			}
		}
		return true;
	}
};

template <class T>
using vector_of_value_map_t = unordered_map<vector<Value>, T, VectorOfValuesHashFunction, VectorOfValuesEquality>;

struct CopyToFileInfo {
	explicit CopyToFileInfo(string file_path_p) : file_path(std::move(file_path_p)) {
	}

	string file_path;
	unique_ptr<CopyFunctionFileStatistics> file_stats;
};

class CopyToFunctionGlobalState : public GlobalSinkState {
public:
	explicit CopyToFunctionGlobalState(ClientContext &context, unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), last_file_offset(0), global_state(std::move(global_state)) {
		max_open_files = ClientConfig::GetConfig(context).partitioned_write_max_open_files;
	}
	StorageLock lock;
	atomic<idx_t> rows_copied;
	atomic<idx_t> last_file_offset;
	unique_ptr<GlobalFunctionData> global_state;
	//! Created directories
	unordered_set<string> created_directories;
	//! shared state for HivePartitionedColumnData
	shared_ptr<GlobalHivePartitionState> partition_state;
	//! File names
	vector<CopyToFileInfo> file_names;
	//! Max open files
	idx_t max_open_files;

	void CreateDir(const string &dir_path, FileSystem &fs) {
		if (created_directories.find(dir_path) != created_directories.end()) {
			// already attempted to create this directory
			return;
		}
		if (!fs.DirectoryExists(dir_path)) {
			fs.CreateDirectory(dir_path);
		}
		created_directories.insert(dir_path);
	}

	string GetOrCreateDirectory(const vector<idx_t> &cols, const vector<string> &names, const vector<Value> &values,
	                            string path, FileSystem &fs) {
		CreateDir(path, fs);
		for (idx_t i = 0; i < cols.size(); i++) {
			const auto &partition_col_name = names[cols[i]];
			const auto &partition_value = values[i];
			string p_dir;
			p_dir += HivePartitioning::Escape(partition_col_name);
			p_dir += "=";
			p_dir += HivePartitioning::Escape(partition_value.ToString());
			path = fs.JoinPath(path, p_dir);
			CreateDir(path, fs);
		}
		return path;
	}

	optional_ptr<CopyFunctionFileStatistics> AddFile(const StorageLockKey &l, const string &file_name,
	                                                 CopyFunctionReturnType return_type) {
		D_ASSERT(l.GetType() == StorageLockType::EXCLUSIVE);
		optional_ptr<CopyFunctionFileStatistics> result;
		CopyToFileInfo file_info(file_name);
		if (return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
			file_info.file_stats = make_uniq<CopyFunctionFileStatistics>();
			result = file_info.file_stats.get();
		}
		file_names.push_back(std::move(file_info));
		return result;
	}

	void FinalizePartition(ClientContext &context, const PhysicalCopyToFile &op, PartitionWriteInfo &info) {
		if (!info.global_state) {
			// already finalized
			return;
		}
		// finalize the partition
		op.function.copy_to_finalize(context, *op.bind_data, *info.global_state);
		info.global_state.reset();
	}

	void FinalizePartitions(ClientContext &context, const PhysicalCopyToFile &op) {
		// finalize any remaining partitions
		for (auto &entry : active_partitioned_writes) {
			FinalizePartition(context, op, *entry.second);
		}
	}

	PartitionWriteInfo &GetPartitionWriteInfo(ExecutionContext &context, const PhysicalCopyToFile &op,
	                                          const vector<Value> &values) {
		auto global_lock = lock.GetExclusiveLock();
		// check if we have already started writing this partition
		auto active_write_entry = active_partitioned_writes.find(values);
		if (active_write_entry != active_partitioned_writes.end()) {
			// we have - continue writing in this partition
			active_write_entry->second->active_writes++;
			return *active_write_entry->second;
		}
		// check if we need to close any writers before we can continue
		if (active_partitioned_writes.size() >= max_open_files) {
			// we need to! try to close writers
			for (auto &entry : active_partitioned_writes) {
				if (entry.second->active_writes == 0) {
					// we can evict this entry - evict the partition
					FinalizePartition(context.client, op, *entry.second);
					++previous_partitions[entry.first];
					active_partitioned_writes.erase(entry.first);
					break;
				}
			}
		}
		idx_t offset = 0;
		auto prev_offset = previous_partitions.find(values);
		if (prev_offset != previous_partitions.end()) {
			offset = prev_offset->second;
		}
		auto &fs = FileSystem::GetFileSystem(context.client);
		// Create a writer for the current file
		auto trimmed_path = op.GetTrimmedPath(context.client);
		string hive_path = GetOrCreateDirectory(op.partition_columns, op.names, values, trimmed_path, fs);
		string full_path(op.filename_pattern.CreateFilename(fs, hive_path, op.file_extension, offset));
		if (op.overwrite_mode == CopyOverwriteMode::COPY_APPEND) {
			// when appending, we first check if the file exists
			while (fs.FileExists(full_path)) {
				// file already exists - re-generate name
				if (!op.filename_pattern.HasUUID()) {
					throw InternalException("CopyOverwriteMode::COPY_APPEND without {uuid} - and file exists");
				}
				full_path = op.filename_pattern.CreateFilename(fs, hive_path, op.file_extension, offset);
			}
		}
		optional_ptr<CopyFunctionFileStatistics> file_stats;
		if (op.return_type != CopyFunctionReturnType::CHANGED_ROWS) {
			file_stats = AddFile(*global_lock, full_path, op.return_type);
		}
		// initialize writes
		auto info = make_uniq<PartitionWriteInfo>();
		info->global_state = op.function.copy_to_initialize_global(context.client, *op.bind_data, full_path);
		if (file_stats) {
			op.function.copy_to_get_written_statistics(context.client, *op.bind_data, *info->global_state, *file_stats);
		}
		auto &result = *info;
		info->active_writes = 1;
		// store in active write map
		active_partitioned_writes.insert(make_pair(values, std::move(info)));
		return result;
	}

	void FinishPartitionWrite(PartitionWriteInfo &info) {
		auto global_lock = lock.GetExclusiveLock();
		info.active_writes--;
	}

private:
	//! The active writes per partition (for partitioned write)
	vector_of_value_map_t<unique_ptr<PartitionWriteInfo>> active_partitioned_writes;
	vector_of_value_map_t<idx_t> previous_partitions;
};

string PhysicalCopyToFile::GetTrimmedPath(ClientContext &context) const {
	auto &fs = FileSystem::GetFileSystem(context);
	string trimmed_path = file_path;
	StringUtil::RTrim(trimmed_path, fs.PathSeparator(trimmed_path));
	return trimmed_path;
}

class CopyToFunctionLocalState : public LocalSinkState {
public:
	explicit CopyToFunctionLocalState(unique_ptr<LocalFunctionData> local_state) : local_state(std::move(local_state)) {
	}
	unique_ptr<GlobalFunctionData> global_state;
	unique_ptr<LocalFunctionData> local_state;

	//! Buffers the tuples in partitions before writing
	unique_ptr<HivePartitionedColumnData> part_buffer;
	unique_ptr<PartitionedColumnDataAppendState> part_buffer_append_state;

	idx_t append_count = 0;

	void InitializeAppendState(ClientContext &context, const PhysicalCopyToFile &op,
	                           CopyToFunctionGlobalState &gstate) {
		part_buffer = make_uniq<HivePartitionedColumnData>(context, op.expected_types, op.partition_columns,
		                                                   gstate.partition_state);
		part_buffer_append_state = make_uniq<PartitionedColumnDataAppendState>();
		part_buffer->InitializeAppendState(*part_buffer_append_state);
		append_count = 0;
	}

	void AppendToPartition(ExecutionContext &context, const PhysicalCopyToFile &op, CopyToFunctionGlobalState &g,
	                       DataChunk &chunk) {
		if (!part_buffer) {
			// re-initialize the append
			InitializeAppendState(context.client, op, g);
		}
		part_buffer->Append(*part_buffer_append_state, chunk);
		append_count += chunk.size();
		if (append_count >= ClientConfig::GetConfig(context.client).partitioned_write_flush_threshold) {
			// flush all cached partitions
			FlushPartitions(context, op, g);
		}
	}

	void ResetAppendState() {
		part_buffer_append_state.reset();
		part_buffer.reset();
		append_count = 0;
	}

	void SetDataWithoutPartitions(DataChunk &chunk, const DataChunk &source, const vector<LogicalType> &col_types,
	                              const vector<idx_t> &part_cols) {
		D_ASSERT(source.ColumnCount() == col_types.size());
		auto types = LogicalCopyToFile::GetTypesWithoutPartitions(col_types, part_cols, false);
		chunk.InitializeEmpty(types);
		set<idx_t> part_col_set(part_cols.begin(), part_cols.end());
		idx_t new_col_id = 0;
		for (idx_t col_idx = 0; col_idx < source.ColumnCount(); col_idx++) {
			if (part_col_set.find(col_idx) == part_col_set.end()) {
				chunk.data[new_col_id].Reference(source.data[col_idx]);
				new_col_id++;
			}
		}
		chunk.SetCardinality(source.size());
	}

	void FlushPartitions(ExecutionContext &context, const PhysicalCopyToFile &op, CopyToFunctionGlobalState &g) {
		if (!part_buffer) {
			return;
		}
		part_buffer->FlushAppendState(*part_buffer_append_state);
		auto &partitions = part_buffer->GetPartitions();
		auto partition_key_map = part_buffer->GetReverseMap();

		for (idx_t i = 0; i < partitions.size(); i++) {
			auto entry = partition_key_map.find(i);
			if (entry == partition_key_map.end()) {
				continue;
			}
			// get the partition write info for this buffer
			auto &info = g.GetPartitionWriteInfo(context, op, entry->second->values);

			auto local_copy_state = op.function.copy_to_initialize_local(context, *op.bind_data);
			// push the chunks into the write state
			for (auto &chunk : partitions[i]->Chunks()) {
				if (op.write_partition_columns) {
					op.function.copy_to_sink(context, *op.bind_data, *info.global_state, *local_copy_state, chunk);
				} else {
					DataChunk filtered_chunk;
					SetDataWithoutPartitions(filtered_chunk, chunk, op.expected_types, op.partition_columns);
					op.function.copy_to_sink(context, *op.bind_data, *info.global_state, *local_copy_state,
					                         filtered_chunk);
				}
			}
			op.function.copy_to_combine(context, *op.bind_data, *info.global_state, *local_copy_state);
			local_copy_state.reset();
			partitions[i].reset();
			g.FinishPartitionWrite(info);
		}
		ResetAppendState();
	}
};

unique_ptr<GlobalFunctionData> PhysicalCopyToFile::CreateFileState(ClientContext &context, GlobalSinkState &sink,
                                                                   StorageLockKey &global_lock) const {
	auto &g = sink.Cast<CopyToFunctionGlobalState>();
	idx_t this_file_offset = g.last_file_offset++;
	auto &fs = FileSystem::GetFileSystem(context);
	string output_path(filename_pattern.CreateFilename(fs, file_path, file_extension, this_file_offset));
	optional_ptr<CopyFunctionFileStatistics> file_stats;
	if (return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		file_stats = g.AddFile(global_lock, output_path, return_type);
	}
	auto result = function.copy_to_initialize_global(context, *bind_data, output_path);
	if (file_stats) {
		function.copy_to_get_written_statistics(context, *bind_data, *result, *file_stats);
	}
	return result;
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	if (partition_output) {
		auto &g = sink_state->Cast<CopyToFunctionGlobalState>();

		auto state = make_uniq<CopyToFunctionLocalState>(nullptr);
		state->InitializeAppendState(context.client, *this, g);
		return std::move(state);
	}
	auto res = make_uniq<CopyToFunctionLocalState>(function.copy_to_initialize_local(context, *bind_data));
	return std::move(res);
}

void CheckDirectory(FileSystem &fs, const string &file_path, CopyOverwriteMode overwrite_mode) {
	if (overwrite_mode == CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE ||
	    overwrite_mode == CopyOverwriteMode::COPY_APPEND) {
		// with overwrite or ignore we fully ignore the presence of any files instead of erasing them
		return;
	}
	if (fs.IsRemoteFile(file_path) && overwrite_mode == CopyOverwriteMode::COPY_OVERWRITE) {
		// we can only remove files for local file systems currently
		// as remote file systems (e.g. S3) do not support RemoveFile
		throw NotImplementedException("OVERWRITE is not supported for remote file systems");
	}
	vector<string> file_list;
	vector<string> directory_list;
	directory_list.push_back(file_path);
	for (idx_t dir_idx = 0; dir_idx < directory_list.size(); dir_idx++) {
		auto directory = directory_list[dir_idx];
		fs.ListFiles(directory, [&](const string &path, bool is_directory) {
			auto full_path = fs.JoinPath(directory, path);
			if (is_directory) {
				directory_list.emplace_back(std::move(full_path));
			} else {
				file_list.emplace_back(std::move(full_path));
			}
		});
	}
	if (file_list.empty()) {
		return;
	}
	if (overwrite_mode == CopyOverwriteMode::COPY_OVERWRITE) {
		for (auto &file : file_list) {
			fs.RemoveFile(file);
		}
	} else {
		throw IOException("Directory \"%s\" is not empty! Enable OVERWRITE option to overwrite files", file_path);
	}
}

unique_ptr<GlobalSinkState> PhysicalCopyToFile::GetGlobalSinkState(ClientContext &context) const {
	if (partition_output || per_thread_output || rotate) {
		auto &fs = FileSystem::GetFileSystem(context);
		if (fs.FileExists(file_path)) {
			// the target file exists AND is a file (not a directory)
			if (fs.IsRemoteFile(file_path)) {
				// for remote files we cannot do anything - as we cannot delete the file
				throw IOException("Cannot write to \"%s\" - it exists and is a file, not a directory!", file_path);
			} else {
				// for local files we can remove the file if OVERWRITE_OR_IGNORE is enabled
				if (overwrite_mode == CopyOverwriteMode::COPY_OVERWRITE) {
					fs.RemoveFile(file_path);
				} else {
					throw IOException("Cannot write to \"%s\" - it exists and is a file, not a directory! Enable "
					                  "OVERWRITE option to overwrite the file",
					                  file_path);
				}
			}
		}
		// what if the target exists and is a directory
		if (!fs.DirectoryExists(file_path)) {
			fs.CreateDirectory(file_path);
		} else {
			CheckDirectory(fs, file_path, overwrite_mode);
		}

		auto state = make_uniq<CopyToFunctionGlobalState>(context, nullptr);
		if (!per_thread_output && rotate) {
			auto global_lock = state->lock.GetExclusiveLock();
			state->global_state = CreateFileState(context, *state, *global_lock);
		}

		if (partition_output) {
			state->partition_state = make_shared_ptr<GlobalHivePartitionState>();
		}

		return std::move(state);
	}

	auto state = make_uniq<CopyToFunctionGlobalState>(
	    context, function.copy_to_initialize_global(context, *bind_data, file_path));
	auto global_lock = state->lock.GetExclusiveLock();
	auto file_stats = state->AddFile(*global_lock, file_path, return_type);
	if (file_stats) {
		function.copy_to_get_written_statistics(context, *bind_data, *state->global_state, *file_stats);
	}
	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
void PhysicalCopyToFile::MoveTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_path = GetNonTmpFile(context, tmp_file_path);
	if (fs.FileExists(file_path)) {
		fs.RemoveFile(file_path);
	}
	fs.MoveFile(tmp_file_path, file_path);
}

string PhysicalCopyToFile::GetNonTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);

	auto path = StringUtil::GetFilePath(tmp_file_path);
	auto base = StringUtil::GetFileName(tmp_file_path);

	auto prefix = base.find("tmp_");
	if (prefix == 0) {
		base = base.substr(4);
	}

	return fs.JoinPath(path, base);
}

PhysicalCopyToFile::PhysicalCopyToFile(vector<LogicalType> types, CopyFunction function_p,
                                       unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::COPY_TO_FILE, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data)), parallel(false) {
}

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &g = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &l = input.local_state.Cast<CopyToFunctionLocalState>();

	g.rows_copied += chunk.size();

	if (partition_output) {
		l.AppendToPartition(context, *this, g, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	if (per_thread_output) {
		auto &gstate = l.global_state;
		if (!gstate) {
			// Lazily create file state here to prevent creating empty files
			auto global_lock = g.lock.GetExclusiveLock();
			gstate = CreateFileState(context.client, *sink_state, *global_lock);
		} else if (rotate && function.rotate_next_file(*gstate, *bind_data, file_size_bytes)) {
			function.copy_to_finalize(context.client, *bind_data, *gstate);
			auto global_lock = g.lock.GetExclusiveLock();
			gstate = CreateFileState(context.client, *sink_state, *global_lock);
		}
		function.copy_to_sink(context, *bind_data, *gstate, *l.local_state, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	if (!file_size_bytes.IsValid() && !rotate) {
		function.copy_to_sink(context, *bind_data, *g.global_state, *l.local_state, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	// FILE_SIZE_BYTES/rotate is set, but threads write to the same file, synchronize using lock
	auto &gstate = g.global_state;
	auto global_lock = g.lock.GetExclusiveLock();
	if (rotate && function.rotate_next_file(*gstate, *bind_data, file_size_bytes)) {
		auto owned_gstate = std::move(gstate);
		gstate = CreateFileState(context.client, *sink_state, *global_lock);
		global_lock.reset();
		function.copy_to_finalize(context.client, *bind_data, *owned_gstate);
	} else {
		global_lock.reset();
	}

	global_lock = g.lock.GetSharedLock();
	function.copy_to_sink(context, *bind_data, *gstate, *l.local_state, chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCopyToFile::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &g = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &l = input.local_state.Cast<CopyToFunctionLocalState>();

	if (partition_output) {
		// flush all remaining partitions
		l.FlushPartitions(context, *this, g);
	} else if (function.copy_to_combine) {
		if (per_thread_output) {
			// For PER_THREAD_OUTPUT, we can combine/finalize immediately (if there is a gstate)
			if (l.global_state) {
				function.copy_to_combine(context, *bind_data, *l.global_state, *l.local_state);
				function.copy_to_finalize(context.client, *bind_data, *l.global_state);
			}
		} else if (rotate) {
			// File in global state may change with FILE_SIZE_BYTES/rotate, need to grab lock
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
	if (partition_output) {
		// finalize any outstanding partitions
		gstate.FinalizePartitions(context, *this);
		return SinkFinalizeType::READY;
	}
	if (per_thread_output) {
		// already happened in combine
		if (NumericCast<int64_t>(gstate.rows_copied.load()) == 0 && sink_state != nullptr) {
			// no rows from source, write schema to file
			auto global_lock = gstate.lock.GetExclusiveLock();
			gstate.global_state = CreateFileState(context, *sink_state, *global_lock);
			function.copy_to_finalize(context, *bind_data, *gstate.global_state);
		}
		return SinkFinalizeType::READY;
	}
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state);

		if (use_tmp_file) {
			D_ASSERT(!per_thread_output);
			D_ASSERT(!partition_output);
			D_ASSERT(!file_size_bytes.IsValid());
			D_ASSERT(!rotate);
			MoveTmpFile(context, file_path);
		}
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

class CopyToFileGlobalSourceState : public GlobalSourceState {
public:
	CopyToFileGlobalSourceState() {
	}

	idx_t offset = 0;

	idx_t MaxThreads() override {
		return 1;
	}
};

unique_ptr<GlobalSourceState> PhysicalCopyToFile::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CopyToFileGlobalSourceState>();
}

void PhysicalCopyToFile::ReturnStatistics(DataChunk &chunk, idx_t row_idx, const string &file_name,
                                          CopyFunctionFileStatistics &file_stats) {
	// filename VARCHAR
	chunk.SetValue(0, row_idx, file_name);
	// count BIGINT
	chunk.SetValue(1, row_idx, Value::UBIGINT(file_stats.row_count));
	// file size bytes BIGINT
	chunk.SetValue(2, row_idx, Value::UBIGINT(file_stats.file_size_bytes));
	// footer offset BIGINT
	chunk.SetValue(3, row_idx, file_stats.footer_offset);
	// footer size BIGINT
	chunk.SetValue(4, row_idx, file_stats.footer_size);
	// column statistics map(varchar, map(varchar, varchar))
	map<string, Value> stats;
	for (auto &entry : file_stats.column_statistics) {
		map<string, Value> per_column_stats;
		for (auto &stats_entry : entry.second) {
			per_column_stats.insert(make_pair(stats_entry.first, stats_entry.second));
		}
		vector<Value> stats_keys;
		vector<Value> stats_values;
		for (auto &stats_entry : per_column_stats) {
			stats_keys.emplace_back(stats_entry.first);
			stats_values.emplace_back(std::move(stats_entry.second));
		}
		auto map_value =
		    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(stats_keys), std::move(stats_values));
		stats.insert(make_pair(entry.first, std::move(map_value)));
	}
	vector<Value> keys;
	vector<Value> values;
	for (auto &entry : stats) {
		keys.emplace_back(entry.first);
		values.emplace_back(std::move(entry.second));
	}
	auto map_val_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	chunk.SetValue(5, row_idx, Value::MAP(LogicalType::VARCHAR, map_val_type, std::move(keys), std::move(values)));
}

SourceResultType PhysicalCopyToFile::GetData(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<CopyToFunctionGlobalState>();
	if (return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
		auto &source_state = input.global_state.Cast<CopyToFileGlobalSourceState>();
		idx_t next_end = MinValue<idx_t>(source_state.offset + STANDARD_VECTOR_SIZE, g.file_names.size());
		idx_t count = next_end - source_state.offset;
		for (idx_t i = 0; i < count; i++) {
			auto &file_entry = g.file_names[source_state.offset + i];
			auto &file_stats = *file_entry.file_stats;
			ReturnStatistics(chunk, i, file_entry.file_path, file_stats);
		}
		chunk.SetCardinality(count);
		source_state.offset += count;
		return source_state.offset < g.file_names.size() ? SourceResultType::HAVE_MORE_OUTPUT
		                                                 : SourceResultType::FINISHED;
	}

	chunk.SetCardinality(1);
	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.rows_copied.load())));
		break;
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST: {
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.rows_copied.load())));
		vector<Value> file_name_list;
		for (auto &file_names : g.file_names) {
			file_name_list.emplace_back(file_names.file_path);
		}
		chunk.SetValue(1, 0, Value::LIST(LogicalType::VARCHAR, std::move(file_name_list)));
		break;
	}
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
