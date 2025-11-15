#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/main/settings.hpp"

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

class CopyToFunctionGlobalState : public GlobalSinkState {
public:
	explicit CopyToFunctionGlobalState(ClientContext &context)
	    : initialized(false), rows_copied(0), last_file_offset(0),
	      file_write_lock_if_rotating(make_uniq<StorageLock>()) {
		max_open_files = DBConfig::GetSetting<PartitionedWriteMaxOpenFilesSetting>(context);
	}

	StorageLock lock;
	atomic<bool> initialized;
	atomic<idx_t> rows_copied;
	atomic<idx_t> last_file_offset;
	unique_ptr<GlobalFunctionData> global_state;
	//! Created directories
	unordered_set<string> created_directories;
	//! shared state for HivePartitionedColumnData
	shared_ptr<GlobalHivePartitionState> partition_state;
	//! Written file info and stats
	vector<unique_ptr<CopyToFileInfo>> written_files;
	//! Max open files
	idx_t max_open_files;
	//! If rotate is true, this lock is used
	unique_ptr<StorageLock> file_write_lock_if_rotating;

	void Initialize(ClientContext &context, const PhysicalCopyToFile &op) {
		if (initialized) {
			return;
		}
		auto write_lock = lock.GetExclusiveLock();
		if (initialized) {
			return;
		}
		// initialize writing to the file
		global_state = op.function.copy_to_initialize_global(context, *op.bind_data, op.file_path);
		if (op.function.initialize_operator) {
			op.function.initialize_operator(*global_state, op);
		}
		auto written_file_info = AddFile(*write_lock, op.file_path, op.return_type);
		if (written_file_info) {
			op.function.copy_to_get_written_statistics(context, *op.bind_data, *global_state,
			                                           *written_file_info->file_stats);
		}
		initialized = true;
	}

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

	string GetOrCreateDirectory(const vector<idx_t> &cols, bool hive_file_pattern, const vector<string> &names,
	                            const vector<Value> &values, string path, FileSystem &fs) {
		CreateDir(path, fs);
		if (hive_file_pattern) {
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
		}
		return path;
	}

	optional_ptr<CopyToFileInfo> AddFile(const StorageLockKey &l, const string &file_name,
	                                     CopyFunctionReturnType return_type) {
		D_ASSERT(l.GetType() == StorageLockType::EXCLUSIVE);
		auto file_info = make_uniq<CopyToFileInfo>(file_name);
		optional_ptr<CopyToFileInfo> result;
		if (return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
			file_info->file_stats = make_uniq<CopyFunctionFileStatistics>();
			result = file_info.get();
		}
		written_files.push_back(std::move(file_info));
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
		if (op.hive_file_pattern) {
			auto prev_offset = previous_partitions.find(values);
			if (prev_offset != previous_partitions.end()) {
				offset = prev_offset->second;
			}
		} else {
			offset = global_offset++;
		}
		auto &fs = FileSystem::GetFileSystem(context.client);
		// Create a writer for the current file
		auto trimmed_path = op.GetTrimmedPath(context.client);
		string hive_path =
		    GetOrCreateDirectory(op.partition_columns, op.hive_file_pattern, op.names, values, trimmed_path, fs);
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
		optional_ptr<CopyToFileInfo> written_file_info;
		if (op.return_type != CopyFunctionReturnType::CHANGED_ROWS) {
			written_file_info = AddFile(*global_lock, full_path, op.return_type);
		}
		// initialize writes
		auto info = make_uniq<PartitionWriteInfo>();
		info->global_state = op.function.copy_to_initialize_global(context.client, *op.bind_data, full_path);
		if (written_file_info) {
			// set up the file stats for the copy
			op.function.copy_to_get_written_statistics(context.client, *op.bind_data, *info->global_state,
			                                           *written_file_info->file_stats);

			// set the partition info
			vector<Value> partition_keys;
			vector<Value> partition_values;
			for (idx_t i = 0; i < op.partition_columns.size(); i++) {
				const auto &partition_col_name = op.names[op.partition_columns[i]];
				const auto &partition_value = values[i];
				partition_keys.emplace_back(partition_col_name);
				partition_values.push_back(partition_value.DefaultCastAs(LogicalType::VARCHAR));
			}
			written_file_info->partition_keys = Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR,
			                                               std::move(partition_keys), std::move(partition_values));
		}
		if (op.function.initialize_operator) {
			op.function.initialize_operator(*info->global_state, op);
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
	idx_t global_offset = 0;
};

string PhysicalCopyToFile::GetTrimmedPath(ClientContext &context) const {
	auto &fs = FileSystem::GetFileSystem(context);
	string trimmed_path = file_path;
	StringUtil::RTrim(trimmed_path, fs.PathSeparator(trimmed_path));
	return trimmed_path;
}

class CopyToFunctionLocalState : public LocalSinkState {
public:
	explicit CopyToFunctionLocalState(ClientContext &context, unique_ptr<LocalFunctionData> local_state)
	    : local_state(std::move(local_state)) {
		partitioned_write_flush_threshold = DBConfig::GetSetting<PartitionedWriteFlushThresholdSetting>(context);
	}
	unique_ptr<GlobalFunctionData> global_state;
	unique_ptr<LocalFunctionData> local_state;
	idx_t total_rows_copied = 0;
	idx_t partitioned_write_flush_threshold;

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
		if (append_count >= partitioned_write_flush_threshold) {
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
	optional_ptr<CopyToFileInfo> written_file_info;
	if (return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		written_file_info = g.AddFile(global_lock, output_path, return_type);
	}
	auto result = function.copy_to_initialize_global(context, *bind_data, output_path);
	if (written_file_info) {
		function.copy_to_get_written_statistics(context, *bind_data, *result, *written_file_info->file_stats);
	}
	if (function.initialize_operator) {
		function.initialize_operator(*result, *this);
	}
	return result;
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	if (partition_output) {
		auto &g = sink_state->Cast<CopyToFunctionGlobalState>();

		auto state = make_uniq<CopyToFunctionLocalState>(context.client, nullptr);
		state->InitializeAppendState(context.client, *this, g);
		return std::move(state);
	}
	auto res =
	    make_uniq<CopyToFunctionLocalState>(context.client, function.copy_to_initialize_local(context, *bind_data));
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
		if (!fs.IsRemoteFile(file_path)) {
			if (fs.FileExists(file_path)) {
				// the target file exists AND is a file (not a directory)
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

		auto state = make_uniq<CopyToFunctionGlobalState>(context);
		if (!per_thread_output && rotate && write_empty_file) {
			auto global_lock = state->lock.GetExclusiveLock();
			state->global_state = CreateFileState(context, *state, *global_lock);
		}

		if (partition_output) {
			state->partition_state = make_shared_ptr<GlobalHivePartitionState>();
		}

		return std::move(state);
	}

	auto state = make_uniq<CopyToFunctionGlobalState>(context);
	if (write_empty_file) {
		// if we are writing the file also if it is empty - initialize now
		state->Initialize(context, *this);
	}
	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
void PhysicalCopyToFile::MoveTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_path = GetNonTmpFile(context, tmp_file_path);
	fs.TryRemoveFile(file_path);
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

PhysicalCopyToFile::PhysicalCopyToFile(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function_p,
                                       unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::COPY_TO_FILE, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data)), parallel(false) {
}

void PhysicalCopyToFile::WriteRotateInternal(ExecutionContext &context, GlobalSinkState &global_state,
                                             const std::function<void(GlobalFunctionData &)> &fun) const {
	auto &g = global_state.Cast<CopyToFunctionGlobalState>();

	// Loop until we can write (synchronize using locks when using parallel writes to the same files and "rotate")
	while (true) {
		// Grab global lock and dereference the current file state (and corresponding lock)
		auto global_guard = g.lock.GetExclusiveLock();
		if (!g.global_state) {
			g.global_state = CreateFileState(context.client, *sink_state, *global_guard);
		}
		auto &file_state = *g.global_state;
		auto &file_lock = *g.file_write_lock_if_rotating;
		if (rotate && function.rotate_next_file(file_state, *bind_data, file_size_bytes)) {
			// Global state must be rotated. Move to local scope, create an new one, and immediately release global lock
			auto owned_gstate = std::move(g.global_state);
			g.global_state = CreateFileState(context.client, *sink_state, *global_guard);
			auto owned_lock = std::move(g.file_write_lock_if_rotating);
			g.file_write_lock_if_rotating = make_uniq<StorageLock>();
			global_guard.reset();

			// This thread now waits for the exclusive lock on this file while other threads complete their writes
			// Note that new writes can still start, as there is already a new global state
			auto file_guard = owned_lock->GetExclusiveLock();
			function.copy_to_finalize(context.client, *bind_data, *owned_gstate);
		} else {
			// Get shared file write lock while holding global lock,
			// so file can't be rotated before we get the write lock
			auto file_guard = file_lock.GetSharedLock();

			// Because we got the shared lock on the file, we're sure that it will keep existing until we release it
			global_guard.reset();

			// Sink/Combine!
			fun(file_state);
			break;
		}
	}
}

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &g = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &l = input.local_state.Cast<CopyToFunctionLocalState>();

	if (!write_empty_file && !rotate) {
		// if we are only writing the file when there are rows to write we need to initialize here
		g.Initialize(context.client, *this);
	}
	l.total_rows_copied += chunk.size();

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

	WriteRotateInternal(context, input.global_state, [&](GlobalFunctionData &gstate) {
		function.copy_to_sink(context, *bind_data, gstate, *l.local_state, chunk);
	});

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCopyToFile::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &g = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &l = input.local_state.Cast<CopyToFunctionLocalState>();
	if (l.total_rows_copied == 0) {
		// no rows copied
		return SinkCombineResultType::FINISHED;
	}
	g.rows_copied += l.total_rows_copied;

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
			WriteRotateInternal(context, input.global_state, [&](GlobalFunctionData &gstate) {
				function.copy_to_combine(context, *bind_data, gstate, *l.local_state);
			});
		} else if (g.global_state) {
			function.copy_to_combine(context, *bind_data, *g.global_state, *l.local_state);
		}
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCopyToFile::FinalizeInternal(ClientContext &context, GlobalSinkState &global_state) const {
	auto &gstate = global_state.Cast<CopyToFunctionGlobalState>();
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
	if (function.copy_to_finalize && gstate.global_state) {
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

SinkFinalizeType PhysicalCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	return FinalizeInternal(context, input.global_state);
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

void PhysicalCopyToFile::ReturnStatistics(DataChunk &chunk, idx_t row_idx, CopyToFileInfo &info) {
	auto &file_stats = *info.file_stats;

	// filename VARCHAR
	chunk.SetValue(0, row_idx, info.file_path);
	// count BIGINT
	chunk.SetValue(1, row_idx, Value::UBIGINT(file_stats.row_count));
	// file size bytes BIGINT
	chunk.SetValue(2, row_idx, Value::UBIGINT(file_stats.file_size_bytes));
	// footer size bytes BIGINT
	chunk.SetValue(3, row_idx, file_stats.footer_size_bytes);
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
	chunk.SetValue(4, row_idx, Value::MAP(LogicalType::VARCHAR, map_val_type, std::move(keys), std::move(values)));

	// partition_keys map(varchar, varchar)
	chunk.SetValue(5, row_idx, info.partition_keys);
}

SourceResultType PhysicalCopyToFile::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<CopyToFunctionGlobalState>();
	if (return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
		auto &source_state = input.global_state.Cast<CopyToFileGlobalSourceState>();
		idx_t next_end = MinValue<idx_t>(source_state.offset + STANDARD_VECTOR_SIZE, g.written_files.size());
		idx_t count = next_end - source_state.offset;
		for (idx_t i = 0; i < count; i++) {
			auto &file_entry = *g.written_files[source_state.offset + i];
			if (use_tmp_file) {
				file_entry.file_path = GetNonTmpFile(context.client, file_entry.file_path);
			}
			ReturnStatistics(chunk, i, file_entry);
		}
		chunk.SetCardinality(count);
		source_state.offset += count;
		return source_state.offset < g.written_files.size() ? SourceResultType::HAVE_MORE_OUTPUT
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
		for (auto &file_info : g.written_files) {
			if (use_tmp_file) {
				file_name_list.emplace_back(GetNonTmpFile(context.client, file_info->file_path));
			} else {
				file_name_list.emplace_back(file_info->file_path);
			}
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
