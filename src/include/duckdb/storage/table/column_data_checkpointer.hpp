//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_data_checkpointer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"

namespace duckdb {
struct TableScanOptions;

//! Holds state related to a single column during compression
struct ColumnDataCheckpointData {
public:
	//! Default constructor used when column data does not need to be checkpointed
	ColumnDataCheckpointData() {
	}
	ColumnDataCheckpointData(ColumnCheckpointState &checkpoint_state, ColumnData &col_data, DatabaseInstance &db,
	                         const RowGroup &row_group, StorageManager &storage_manager)
	    : checkpoint_state(checkpoint_state), col_data(col_data), db(db), row_group(row_group),
	      storage_manager(storage_manager) {
	}

public:
	const CompressionFunction &GetCompressionFunction(CompressionType type);
	const LogicalType &GetType() const;
	ColumnData &GetColumnData();
	const RowGroup &GetRowGroup();
	ColumnCheckpointState &GetCheckpointState();
	DatabaseInstance &GetDatabase();
	StorageManager &GetStorageManager();

private:
	optional_ptr<ColumnCheckpointState> checkpoint_state;
	optional_ptr<ColumnData> col_data;
	optional_ptr<DatabaseInstance> db;
	optional_ptr<const RowGroup> row_group;
	optional_ptr<StorageManager> storage_manager;
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
	~ColumnDataCheckpointer();

public:
	void Checkpoint();
	void FinalizeCheckpoint();

private:
	struct BaseSegmentCheckpointPlan;

	void ScanSegments(const ColumnData &column_data, const std::function<void(Vector &)> &callback);
	void ScanSegment(const ColumnData &column_data, SegmentNode<ColumnSegment> &segment,
	                 const std::function<void(Vector &)> &callback);
	vector<unique_ptr<AnalyzeState>> InitAnalyze(idx_t checkpoint_state_idx);
	void AnalyzeVector(idx_t checkpoint_state_idx, vector<unique_ptr<AnalyzeState>> &states, Vector &scan_vector);
	CheckpointAnalyzeResult FinalizeAnalyze(idx_t checkpoint_state_idx, vector<unique_ptr<AnalyzeState>> states,
	                                        CompressionType forced_method);
	unique_ptr<BaseSegmentCheckpointPlan> TryBuildBaseSegmentPlan();
	vector<CheckpointAnalyzeResult> DetectBestCompressionMethod();
	void WriteToDisk();
	void WriteBaseSegments();
	void WriteValidity(CheckpointAnalyzeResult &analyze_result);
	void WritePersistentSegments(ColumnCheckpointState &state);
	void PreserveUnchangedValidity();
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
	unique_ptr<BaseSegmentCheckpointPlan> base_segment_plan;
};

} // namespace duckdb
