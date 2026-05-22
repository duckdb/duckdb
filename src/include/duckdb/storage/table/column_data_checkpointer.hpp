//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_data_checkpointer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/checkpoint/string_checkpoint_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"

#include <functional>

namespace duckdb {
struct TableScanOptions;

//! Holds state related to a single column during compression
struct ColumnDataCheckpointData {
public:
	//! Default constructor used when column data does not need to be checkpointed
	ColumnDataCheckpointData() {
	}
	ColumnDataCheckpointData(ColumnCheckpointState &checkpoint_state, LogicalType type, DatabaseInstance &db,
	                         StorageManager &storage_manager)
	    : checkpoint_state(checkpoint_state), type(std::move(type)), db(db), storage_manager(storage_manager),
	      storage_version(storage_manager.GetStorageVersion()) {
	}

public:
	using OverflowStringWriterFactory = std::function<unique_ptr<OverflowStringWriter>()>;
	using FlushSegmentFn = std::function<void(unique_ptr<ColumnSegment>, BufferHandle, idx_t)>;
	using FlushSegmentInternalFn = std::function<void(unique_ptr<ColumnSegment>, idx_t)>;

	ColumnDataCheckpointData(LogicalType type, DatabaseInstance &db, StorageVersion storage_version,
	                         OverflowStringWriterFactory overflow_writer_factory, FlushSegmentFn flush_segment_fn,
	                         FlushSegmentInternalFn flush_segment_internal_fn, BlockManager &block_manager)
	    : type(std::move(type)), db(db), storage_version(storage_version),
	      overflow_writer_factory(std::move(overflow_writer_factory)), flush_segment_fn(std::move(flush_segment_fn)),
	      flush_segment_internal_fn(std::move(flush_segment_internal_fn)), block_manager(block_manager) {
	}

	const CompressionFunction &GetCompressionFunction(CompressionType type);
	const LogicalType &GetType() const;
	ColumnCheckpointState &GetCheckpointState();
	DatabaseInstance &GetDatabase();
	StorageManager &GetStorageManager();
	bool HasStorageManager() const noexcept {
		return static_cast<bool>(storage_manager);
	}
	StorageVersion GetStorageVersion() const {
		return storage_version;
	}
	bool HasOverflowStringWriterFactory() const noexcept {
		return static_cast<bool>(overflow_writer_factory);
	}
	unique_ptr<OverflowStringWriter> MakeOverflowStringWriter() const {
		return overflow_writer_factory();
	}
	void FlushSegment(unique_ptr<ColumnSegment> segment, BufferHandle handle, idx_t segment_size);
	void FlushSegmentInternal(unique_ptr<ColumnSegment> segment, idx_t segment_size);
	BlockManager &GetBlockManager();
	block_id_t GetFreeBlockId();

private:
	optional_ptr<ColumnCheckpointState> checkpoint_state;
	LogicalType type;
	optional_ptr<DatabaseInstance> db;
	optional_ptr<StorageManager> storage_manager;
	StorageVersion storage_version;
	OverflowStringWriterFactory overflow_writer_factory;
	FlushSegmentFn flush_segment_fn;
	FlushSegmentInternalFn flush_segment_internal_fn;
	optional_ptr<BlockManager> block_manager;
};

struct CheckpointAnalyzeResult {
public:
	//! Default constructor, returned when the column data doesn't require checkpoint
	CheckpointAnalyzeResult() {
	}
	CheckpointAnalyzeResult(unique_ptr<AnalyzeState> &&analyze_state, const CompressionFunction &function)
	    : analyze_state(std::move(analyze_state)), function(function) {
	}

public:
	unique_ptr<AnalyzeState> analyze_state;
	optional_ptr<const CompressionFunction> function;
};

class ColumnDataCheckpointer {
public:
	ColumnDataCheckpointer(vector<reference<ColumnCheckpointState>> &states, StorageManager &storage_manager,
	                       const RowGroup &row_group, ColumnCheckpointInfo &checkpoint_info);

public:
	void Checkpoint();
	void FinalizeCheckpoint();

private:
	void ScanSegments(const std::function<void(Vector &)> &callback);
	vector<CheckpointAnalyzeResult> DetectBestCompressionMethod();
	void WriteToDisk();
	void WritePersistentSegments(ColumnCheckpointState &state);
	bool HasChanges(ColumnData &col_data);
	void InitAnalyze();
	void DropSegments();
	bool ValidityCoveredByBasedata(vector<CheckpointAnalyzeResult> &result);

private:
	vector<reference<ColumnCheckpointState>> &checkpoint_states;
	StorageManager &storage_manager;
	const RowGroup &row_group;
	DataChunk intermediate;
	ColumnCheckpointInfo &checkpoint_info;

	bool has_changes = false;
	//! For every column data that is being checkpointed, the applicable functions
	vector<vector<optional_ptr<const CompressionFunction>>> compression_functions;
	//! For every column data that is being checkpointed, the analyze state of functions being tried
	vector<vector<unique_ptr<AnalyzeState>>> analyze_states;
};

} // namespace duckdb
