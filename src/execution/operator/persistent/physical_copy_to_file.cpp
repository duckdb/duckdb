#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/main/settings.hpp"
#include "fmt/format.h"

#include <algorithm>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Util
//===--------------------------------------------------------------------===//
enum class PhysicalCopyFlushBatchType : uint8_t { SINK, COMBINE, FINALIZE };

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

void CheckDirectory(FileSystem &fs, const string &file_path, CopyOverwriteMode overwrite_mode) {
	if (overwrite_mode == CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE ||
	    overwrite_mode == CopyOverwriteMode::COPY_APPEND) {
		// with overwrite or ignore we fully ignore the presence of any files instead of erasing them
		return;
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
		fs.RemoveFiles(file_list);
	} else {
		throw IOException("Directory \"%s\" is not empty! Enable OVERWRITE option to overwrite files", file_path);
	}
}

struct PhysicalCopyToFileColumnStatsMapData {
	vector<Value> keys;
	vector<Value> values;
};

static PhysicalCopyToFileColumnStatsMapData
CreateColumnStatistics(const case_insensitive_map_t<case_insensitive_map_t<Value>> &column_statistics) {
	PhysicalCopyToFileColumnStatsMapData result;

	//! Use a map to make sure the result has a consistent ordering
	map<string, Value> stats;
	for (auto &entry : column_statistics) {
		map<string, Value> per_column_stats;
		for (auto &stats_entry : entry.second) {
			per_column_stats.emplace(stats_entry.first, stats_entry.second);
		}
		vector<Value> stats_keys;
		vector<Value> stats_values;
		for (auto &stats_entry : per_column_stats) {
			stats_keys.emplace_back(stats_entry.first);
			stats_values.emplace_back(std::move(stats_entry.second));
		}
		auto map_value =
		    Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(stats_keys), std::move(stats_values));
		stats.emplace(entry.first, std::move(map_value));
	}
	for (auto &entry : stats) {
		result.keys.emplace_back(entry.first);
		result.values.emplace_back(std::move(entry.second));
	}
	return result;
}

struct GlobalFileState {
	explicit GlobalFileState(unique_ptr<GlobalFunctionData> data_p) : data(std::move(data_p)), num_batches(0) {
	}
	unique_ptr<GlobalFunctionData> data;
	atomic<idx_t> num_batches;
};

//===--------------------------------------------------------------------===//
// CopyToFunctionGlobalState
//===--------------------------------------------------------------------===//
class CopyToFunctionGlobalState : public GlobalSinkState {
public:
	CopyToFunctionGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p);
	~CopyToFunctionGlobalState() override;

public:
	void Initialize();

	string GetOrCreateDirectory(const vector<idx_t> &cols, bool hive_file_pattern, const vector<string> &names,
	                            const vector<Value> &values, string path, FileSystem &fs);
	optional_ptr<CopyToFileInfo> AddFile(const StorageLockKey &l, const string &file_name,
	                                     CopyFunctionReturnType return_type);

	void FinalizePartitions();
	PartitionWriteInfo &GetPartitionWriteInfo(const vector<Value> &values);
	void FinishPartitionWrite(PartitionWriteInfo &info);

private:
	void CreateDir(const string &dir_path, FileSystem &fs);
	void FinalizePartition(PartitionWriteInfo &info);

public:
	const PhysicalCopyToFile &op;
	ClientContext &context;

	//! Lock guarding the global state
	StorageLock lock;
	//! Whether the copy was successfully initialized/finalized
	atomic<bool> initialized;
	bool finalized;

	//! The (current) global state
	unique_ptr<GlobalFileState> global_state;
	//! Shared state for HivePartitionedColumnData
	shared_ptr<GlobalHivePartitionState> partition_state;

	//! Lock for more fine-grained locking when rotating files
	unique_ptr<StorageLock> file_write_lock_if_rotating;

	//! The final batch
	annotated_mutex last_batch_lock;
	unique_ptr<ColumnDataCollection> last_batch DUCKDB_GUARDED_BY(last_batch_lock);

	//! Created directories
	unordered_set<string> created_directories;
	//! The list of files created by this operator
	vector<string> created_files;
	//! Written file info and stats
	vector<unique_ptr<CopyToFileInfo>> written_files;

	atomic<idx_t> rows_copied;
	atomic<idx_t> last_file_offset;

private:
	//! The active writes per partition (for partitioned write)
	vector_of_value_map_t<unique_ptr<PartitionWriteInfo>> active_partitioned_writes;
	vector_of_value_map_t<idx_t> previous_partitions;
	idx_t global_offset = 0;
};

CopyToFunctionGlobalState::CopyToFunctionGlobalState(const PhysicalCopyToFile &op_p, ClientContext &context_p)
    : op(op_p), context(context_p), initialized(false), finalized(false),
      file_write_lock_if_rotating(make_uniq<StorageLock>()), rows_copied(0), last_file_offset(0) {
}

CopyToFunctionGlobalState::~CopyToFunctionGlobalState() {
	if (!initialized || finalized || created_files.empty()) {
		return;
	}
	// If we reach here, the query failed before Finalize was called
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &file : created_files) {
		try {
			fs.TryRemoveFile(file);
		} catch (...) {
			// TryRemoveFile migth fail for a varieaty of reasons, but we can't really propagate error codes here, so
			// best effort cleanup
		}
	}
}

void CopyToFunctionGlobalState::Initialize() {
	if (initialized) {
		return;
	}
	auto write_lock = lock.GetExclusiveLock();
	if (initialized) {
		return;
	}
	// initialize writing to the file
	created_files.push_back(op.file_path);
	global_state =
	    make_uniq<GlobalFileState>(op.function.copy_to_initialize_global(context, *op.bind_data, op.file_path));
	if (op.function.initialize_operator) {
		op.function.initialize_operator(*global_state->data, op);
	}
	auto written_file_info = AddFile(*write_lock, op.file_path, op.return_type);
	if (written_file_info) {
		op.function.copy_to_get_written_statistics(context, *op.bind_data, *global_state->data,
		                                           *written_file_info->file_stats);
	}
	initialized = true;
}

void CopyToFunctionGlobalState::CreateDir(const string &dir_path, FileSystem &fs) {
	if (created_directories.find(dir_path) != created_directories.end()) {
		// already attempted to create this directory
		return;
	}
	if (!fs.DirectoryExists(dir_path)) {
		fs.CreateDirectory(dir_path);
	}
	created_directories.insert(dir_path);
}

string CopyToFunctionGlobalState::GetOrCreateDirectory(const vector<idx_t> &cols, bool hive_file_pattern,
                                                       const vector<string> &names, const vector<Value> &values,
                                                       string path, FileSystem &fs) {
	CreateDir(path, fs);
	if (hive_file_pattern) {
		for (idx_t i = 0; i < cols.size(); i++) {
			const auto &partition_col_name = names[cols[i]];
			const auto &partition_value = values[i];
			string p_dir;
			p_dir += HivePartitioning::Escape(partition_col_name);
			p_dir += "=";
			if (partition_value.IsNull()) {
				p_dir += "__HIVE_DEFAULT_PARTITION__";
			} else {
				p_dir += HivePartitioning::Escape(partition_value.ToString());
			}
			path = fs.JoinPath(path, p_dir);
			CreateDir(path, fs);
		}
	}
	return path;
}

optional_ptr<CopyToFileInfo> CopyToFunctionGlobalState::AddFile(const StorageLockKey &l, const string &file_name,
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

void CopyToFunctionGlobalState::FinalizePartition(PartitionWriteInfo &info) {
	if (!info.global_state) {
		// already finalized
		return;
	}
	// finalize the partition
	op.function.copy_to_finalize(context, *op.bind_data, *info.global_state);
	info.global_state.reset();
}

void CopyToFunctionGlobalState::FinalizePartitions() {
	// finalize any remaining partitions
	for (auto &entry : active_partitioned_writes) {
		FinalizePartition(*entry.second);
	}
}

PartitionWriteInfo &CopyToFunctionGlobalState::GetPartitionWriteInfo(const vector<Value> &values) {
	auto global_lock = lock.GetExclusiveLock();
	// check if we have already started writing this partition
	auto active_write_entry = active_partitioned_writes.find(values);
	if (active_write_entry != active_partitioned_writes.end()) {
		// we have - continue writing in this partition
		active_write_entry->second->active_writes++;
		return *active_write_entry->second;
	}
	// check if we need to close any writers before we can continue
	if (active_partitioned_writes.size() >= Settings::Get<PartitionedWriteMaxOpenFilesSetting>(context)) {
		// we need to! try to close writers
		for (auto &entry : active_partitioned_writes) {
			if (entry.second->active_writes == 0) {
				// we can evict this entry - evict the partition
				FinalizePartition(*entry.second);
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
	auto &fs = FileSystem::GetFileSystem(context);
	// Create a writer for the current file
	auto trimmed_path = op.GetTrimmedPath(context, op.file_path);
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
	created_files.push_back(full_path);
	optional_ptr<CopyToFileInfo> written_file_info;
	if (op.return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		written_file_info = AddFile(*global_lock, full_path, op.return_type);
	}
	// initialize writes
	auto info = make_uniq<PartitionWriteInfo>();
	info->global_state = op.function.copy_to_initialize_global(context, *op.bind_data, full_path);
	if (written_file_info) {
		// set up the file stats for the copy
		op.function.copy_to_get_written_statistics(context, *op.bind_data, *info->global_state,
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

void CopyToFunctionGlobalState::FinishPartitionWrite(PartitionWriteInfo &info) {
	auto global_lock = lock.GetExclusiveLock();
	info.active_writes--;
}

//===--------------------------------------------------------------------===//
// CopyToFunctionLocalState
//===--------------------------------------------------------------------===//
class CopyToFunctionLocalState : public LocalSinkState {
public:
	CopyToFunctionLocalState(const PhysicalCopyToFile &op_p, ExecutionContext &context_p,
	                         CopyToFunctionGlobalState &gstate_p, unique_ptr<LocalFunctionData> local_state)
	    : op(op_p), context(context_p), gstate(gstate_p), local_state(std::move(local_state)) {
		partitioned_write_flush_threshold = Settings::Get<PartitionedWriteFlushThresholdSetting>(context.client);
	}

public:
	//! Partitioned append functions
	void InitializePartitionedAppendState();
	void AppendToPartition(DataChunk &chunk);
	void ResetPartitionedAppendState();
	void SetDataWithoutPartitions(DataChunk &chunk, const DataChunk &source, const vector<LogicalType> &col_types,
	                              const vector<idx_t> &part_cols);
	void FlushPartitions();

public:
	const PhysicalCopyToFile &op;
	ExecutionContext &context;
	CopyToFunctionGlobalState &gstate;

	unique_ptr<GlobalFileState> global_state;
	unique_ptr<LocalFunctionData> local_state;

	//! For prepare/flush batch
	unique_ptr<ColumnDataCollection> batch;
	ColumnDataAppendState batch_append_state;

	idx_t total_rows_copied = 0;
	idx_t partitioned_write_flush_threshold;

	//! Buffers the tuples in partitions before writing
	unique_ptr<HivePartitionedColumnData> part_buffer;
	unique_ptr<PartitionedColumnDataAppendState> part_buffer_append_state;

	idx_t append_count = 0;
};

void CopyToFunctionLocalState::InitializePartitionedAppendState() {
	part_buffer = make_uniq<HivePartitionedColumnData>(context.client, op.expected_types, op.partition_columns,
	                                                   gstate.partition_state);
	part_buffer_append_state = make_uniq<PartitionedColumnDataAppendState>();
	part_buffer->InitializeAppendState(*part_buffer_append_state);
	append_count = 0;
}

void CopyToFunctionLocalState::AppendToPartition(DataChunk &chunk) {
	if (!part_buffer) {
		// re-initialize the append
		InitializePartitionedAppendState();
	}
	part_buffer->Append(*part_buffer_append_state, chunk);
	append_count += chunk.size();
	if (append_count >= partitioned_write_flush_threshold) {
		// flush all cached partitions
		FlushPartitions();
	}
}

void CopyToFunctionLocalState::ResetPartitionedAppendState() {
	part_buffer_append_state.reset();
	part_buffer.reset();
	append_count = 0;
}

void CopyToFunctionLocalState::SetDataWithoutPartitions(DataChunk &chunk, const DataChunk &source,
                                                        const vector<LogicalType> &col_types,
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

void CopyToFunctionLocalState::FlushPartitions() {
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
		auto &info = gstate.GetPartitionWriteInfo(entry->second->values);

		auto local_copy_state = op.function.copy_to_initialize_local(context, *op.bind_data);
		// push the chunks into the write state
		for (auto &chunk : partitions[i]->Chunks()) {
			if (op.write_partition_columns) {
				op.function.copy_to_sink(context, *op.bind_data, *info.global_state, *local_copy_state, chunk);
			} else {
				DataChunk filtered_chunk;
				SetDataWithoutPartitions(filtered_chunk, chunk, op.expected_types, op.partition_columns);
				op.function.copy_to_sink(context, *op.bind_data, *info.global_state, *local_copy_state, filtered_chunk);
			}
		}
		op.function.copy_to_combine(context, *op.bind_data, *info.global_state, *local_copy_state);
		local_copy_state.reset();
		partitions[i].reset();
		gstate.FinishPartitionWrite(info);
	}
	ResetPartitionedAppendState();
}

//===--------------------------------------------------------------------===//
// PhysicalCopyToFile
//===--------------------------------------------------------------------===//
PhysicalCopyToFile::PhysicalCopyToFile(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function_p,
                                       unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::COPY_TO_FILE, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data)), parallel(false) {
}

string PhysicalCopyToFile::GetTrimmedPath(ClientContext &context, const string &file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	string trimmed_path = file_path;
	StringUtil::RTrim(trimmed_path, fs.PathSeparator(trimmed_path));
	return trimmed_path;
}

void PhysicalCopyToFile::MoveTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto file_path = GetNonTmpFile(context, tmp_file_path);
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
	auto column_stats = CreateColumnStatistics(file_stats.column_statistics);
	auto map_val_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	chunk.SetValue(
	    4, row_idx,
	    Value::MAP(LogicalType::VARCHAR, map_val_type, std::move(column_stats.keys), std::move(column_stats.values)));

	// partition_keys map(varchar, varchar)
	chunk.SetValue(5, row_idx, info.partition_keys);
}

unique_ptr<GlobalFunctionData> PhysicalCopyToFile::CreateFileState(ClientContext &context, GlobalSinkState &sink,
                                                                   StorageLockKey &global_lock) const {
	auto &gstate = sink.Cast<CopyToFunctionGlobalState>();
	idx_t this_file_offset = gstate.last_file_offset++;
	auto &fs = FileSystem::GetFileSystem(context);
	string output_path(filename_pattern.CreateFilename(fs, file_path, file_extension, this_file_offset));
	gstate.created_files.push_back(output_path);
	optional_ptr<CopyToFileInfo> written_file_info;
	if (return_type != CopyFunctionReturnType::CHANGED_ROWS) {
		written_file_info = gstate.AddFile(global_lock, output_path, return_type);
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

bool PhysicalCopyToFile::Rotate() const {
	return file_size_bytes.IsValid() || batches_per_file.IsValid();
}

bool PhysicalCopyToFile::RotateNow(GlobalFileState &global_state) const {
	if (file_size_bytes.IsValid()) {
		return function.file_size_bytes(*global_state.data) >= file_size_bytes.GetIndex();
	}
	if (batches_per_file.IsValid()) {
		return global_state.num_batches >= batches_per_file.GetIndex();
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Sink Interface
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalCopyToFile::GetGlobalSinkState(ClientContext &context) const {
	if (partition_output || per_thread_output || Rotate()) {
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

		auto state = make_uniq<CopyToFunctionGlobalState>(*this, context);
		if (!per_thread_output && Rotate() && write_empty_file) {
			auto global_lock = state->lock.GetExclusiveLock();
			state->global_state = make_uniq<GlobalFileState>(CreateFileState(context, *state, *global_lock));
		}

		if (partition_output) {
			state->partition_state = make_shared_ptr<GlobalHivePartitionState>();
		}

		return std::move(state);
	}

	auto state = make_uniq<CopyToFunctionGlobalState>(*this, context);
	if (write_empty_file) {
		// if we are writing the file also if it is empty - initialize now
		state->Initialize();
	}
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	auto &gstate = sink_state->Cast<CopyToFunctionGlobalState>();
	if (partition_output) {
		auto lstate = make_uniq<CopyToFunctionLocalState>(*this, context, gstate, nullptr);
		lstate->InitializePartitionedAppendState();
		return std::move(lstate);
	}
	auto res = make_uniq<CopyToFunctionLocalState>(*this, context, gstate,
	                                               function.copy_to_initialize_local(context, *bind_data));
	return std::move(res);
}

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &lstate = input.local_state.Cast<CopyToFunctionLocalState>();

	if (!write_empty_file && !Rotate()) {
		// if we are only writing the file when there are rows to write we need to initialize here
		gstate.Initialize();
	}
	lstate.total_rows_copied += chunk.size();

	if (partition_output) {
		lstate.AppendToPartition(chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	if (!lstate.batch) {
		lstate.batch = make_uniq<ColumnDataCollection>(context.client, expected_types);
		lstate.batch->InitializeAppend(lstate.batch_append_state);
	}
	lstate.batch->Append(lstate.batch_append_state, chunk);

	if (CopyFunctionMustFlushBatch(*lstate.batch, batch_size, batch_size_bytes)) {
		lstate.batch_append_state.current_chunk_state.handles.clear();
		auto &file_state_ptr = per_thread_output ? lstate.global_state : gstate.global_state;
		FlushBatch(context.client, gstate, file_state_ptr, lstate.local_state, std::move(lstate.batch),
		           PhysicalCopyFlushBatchType::SINK);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCopyToFile::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &lstate = input.local_state.Cast<CopyToFunctionLocalState>();
	if (lstate.total_rows_copied == 0) {
		// no rows copied
		return SinkCombineResultType::FINISHED;
	}
	gstate.rows_copied += lstate.total_rows_copied;

	if (partition_output) {
		// flush all remaining partitions
		lstate.FlushPartitions();
		return SinkCombineResultType::FINISHED;
	}

	if (per_thread_output) {
		if (lstate.batch) {
			FlushBatch(context.client, gstate, lstate.global_state, lstate.local_state, std::move(lstate.batch),
			           PhysicalCopyFlushBatchType::COMBINE);
		}
		function.copy_to_finalize(context.client, *bind_data, *lstate.global_state->data);
		return SinkCombineResultType::FINISHED;
	}

	if (!lstate.batch) {
		return SinkCombineResultType::FINISHED;
	}
	D_ASSERT(!CopyFunctionMustFlushBatch(*lstate.batch, batch_size, batch_size_bytes));

	unique_ptr<ColumnDataCollection> batch;
	{
		annotated_lock_guard<annotated_mutex> guard(gstate.last_batch_lock);
		if (gstate.last_batch) {
			D_ASSERT(!CopyFunctionMustFlushBatch(*gstate.last_batch, batch_size, batch_size_bytes));
			const auto count = gstate.last_batch->Count() + lstate.batch->Count();
			const auto size_in_bytes = gstate.last_batch->SizeInBytes() + lstate.batch->SizeInBytes();
			if (CopyFunctionMustFlushBatch(count, size_in_bytes, batch_size, batch_size_bytes)) {
				// Combining makes us overshoot, make sure the smallest one gets flushed now
				auto &small = lstate.batch->Count() < gstate.last_batch->Count() ? lstate.batch : gstate.last_batch;
				auto &large = lstate.batch->Count() < gstate.last_batch->Count() ? gstate.last_batch : lstate.batch;
				batch = std::move(large);
				gstate.last_batch = std::move(small);
			} else {
				gstate.last_batch->Combine(*lstate.batch);
			}
		} else {
			gstate.last_batch = std::move(lstate.batch);
		}
	}

	if (!batch) {
		return SinkCombineResultType::FINISHED;
	}

	auto &file_state_ptr = per_thread_output ? lstate.global_state : gstate.global_state;
	FlushBatch(context.client, gstate, file_state_ptr, lstate.local_state, std::move(batch),
	           PhysicalCopyFlushBatchType::COMBINE);

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto result = FinalizeInternal(context, input.global_state);
	gstate.finalized = true;
	return result;
}

SinkFinalizeType PhysicalCopyToFile::FinalizeInternal(ClientContext &context, GlobalSinkState &global_state) const {
	auto &gstate = global_state.Cast<CopyToFunctionGlobalState>();

	if (partition_output) {
		// finalize any outstanding partitions
		gstate.FinalizePartitions();
		return SinkFinalizeType::READY;
	}

	if (per_thread_output) {
		// already happened in combine
		if (NumericCast<int64_t>(gstate.rows_copied.load()) == 0 && sink_state != nullptr) {
			// no rows from source, write schema to file
			auto global_lock = gstate.lock.GetExclusiveLock();
			gstate.global_state = make_uniq<GlobalFileState>(CreateFileState(context, *sink_state, *global_lock));
			function.copy_to_finalize(context, *bind_data, *gstate.global_state->data);
		}
		return SinkFinalizeType::READY;
	}

	annotated_lock_guard<annotated_mutex> guard(gstate.last_batch_lock);
	if (gstate.last_batch) {
		unique_ptr<LocalFunctionData> lstate;
		FlushBatch(context, gstate, gstate.global_state, lstate, std::move(gstate.last_batch),
		           PhysicalCopyFlushBatchType::FINALIZE);
	}

	if (function.copy_to_finalize && gstate.global_state) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state->data);

		if (use_tmp_file) {
			D_ASSERT(!per_thread_output);
			D_ASSERT(!partition_output);
			D_ASSERT(!file_size_bytes.IsValid());
			D_ASSERT(!Rotate());
			MoveTmpFile(context, file_path);
		}
	}

	return SinkFinalizeType::READY;
}

void PhysicalCopyToFile::FlushBatch(ClientContext &context, GlobalSinkState &gstate_p,
                                    unique_ptr<GlobalFileState> &file_state_ptr, unique_ptr<LocalFunctionData> &lstate,
                                    unique_ptr<ColumnDataCollection> batch,
                                    const PhysicalCopyFlushBatchType flush_batch_type) const {
	auto &gstate = gstate_p.Cast<CopyToFunctionGlobalState>();

	while (true) {
		// Grab global lock and dereference the current file state (and corresponding lock)
		auto global_guard = gstate.lock.GetExclusiveLock();
		if (!file_state_ptr) {
			file_state_ptr = make_uniq<GlobalFileState>(CreateFileState(context, *sink_state, *global_guard));
		}
		auto &file_state = *file_state_ptr;
		auto &file_lock = *gstate.file_write_lock_if_rotating;
		if (RotateNow(file_state)) {
			// Global state must be rotated. Move to local scope, create an new one, and immediately release global lock
			auto owned_file_state = std::move(file_state_ptr);
			file_state_ptr = make_uniq<GlobalFileState>(CreateFileState(context, *sink_state, *global_guard));
			auto owned_lock = std::move(gstate.file_write_lock_if_rotating);
			gstate.file_write_lock_if_rotating = make_uniq<StorageLock>();
			global_guard.reset();

			// This thread now waits for the exclusive lock on this file while other threads complete their writes
			// Note that new writes can still start, as there is already a new global state
			auto file_guard = owned_lock->GetExclusiveLock();
			function.copy_to_finalize(context, *bind_data, *owned_file_state->data);
		} else {
			// We are going to flush a batch to this file, increment counter as soon as possible
			file_state.num_batches++;

			// Get shared file write lock while holding global lock,
			// so file can't be rotated before we get the write lock
			auto file_guard = file_lock.GetSharedLock();

			// Because we got the shared lock on the file, we're sure that it will keep existing until we release it
			global_guard.reset();

			if (function.prepare_batch && function.flush_batch) {
				auto prepared_batch = function.prepare_batch(context, *bind_data, *file_state.data, std::move(batch));
				function.flush_batch(context, *bind_data, *file_state.data, *prepared_batch);
			} else {
				// Old API - make dummy stuff and manually prepare/flush batch
				ThreadContext thread_context(context);
				ExecutionContext execution_context(context, thread_context, nullptr);
				if (!lstate) {
					lstate = function.copy_to_initialize_local(execution_context, *bind_data);
				}
				for (auto &chunk : batch->Chunks()) {
					function.copy_to_sink(execution_context, *bind_data, *file_state.data, *lstate, chunk);
				}
				if (flush_batch_type != PhysicalCopyFlushBatchType::SINK) {
					function.copy_to_combine(execution_context, *bind_data, *file_state.data, *lstate);
				}
			}

			break;
		}
	}
}

//===--------------------------------------------------------------------===//
// Source Interface
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

SourceResultType PhysicalCopyToFile::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<CopyToFunctionGlobalState>();
	if (return_type == CopyFunctionReturnType::WRITTEN_FILE_STATISTICS) {
		auto &source_state = input.global_state.Cast<CopyToFileGlobalSourceState>();
		idx_t next_end = MinValue<idx_t>(source_state.offset + STANDARD_VECTOR_SIZE, gstate.written_files.size());
		idx_t count = next_end - source_state.offset;
		for (idx_t i = 0; i < count; i++) {
			auto &file_entry = *gstate.written_files[source_state.offset + i];
			if (use_tmp_file) {
				file_entry.file_path = GetNonTmpFile(context.client, file_entry.file_path);
			}
			ReturnStatistics(chunk, i, file_entry);
		}
		chunk.SetCardinality(count);
		source_state.offset += count;
		return source_state.offset < gstate.written_files.size() ? SourceResultType::HAVE_MORE_OUTPUT
		                                                         : SourceResultType::FINISHED;
	}

	chunk.SetCardinality(1);
	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_copied.load())));
		break;
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST: {
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.rows_copied.load())));
		vector<Value> file_name_list;
		for (auto &file_info : gstate.written_files) {
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
