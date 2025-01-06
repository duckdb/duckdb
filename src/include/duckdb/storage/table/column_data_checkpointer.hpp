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
	ColumnDataCheckpointData(ColumnCheckpointState &checkpoint_state, ColumnData &col_data, DatabaseInstance &db,
	                         RowGroup &row_group, bool has_changes, ColumnCheckpointInfo &checkpoint_info)
	    : checkpoint_state(checkpoint_state), col_data(col_data), db(db), row_group(row_group),
	      has_changes(has_changes), checkpoint_info(checkpoint_info) {
	}

public:
	CompressionFunction &GetCompressionFunction(CompressionType type);
	const LogicalType &GetType() const;
	ColumnData &GetColumnData();
	RowGroup &GetRowGroup();
	ColumnCheckpointState &GetCheckpointState();
	DatabaseInstance &GetDatabase();

public:
	ColumnCheckpointState &checkpoint_state;
	ColumnData &col_data;
	DatabaseInstance &db;
	RowGroup &row_group;
	bool has_changes;
	ColumnCheckpointInfo &checkpoint_info;
};

class ColumnDataCheckpointer {
public:
	ColumnDataCheckpointer(ColumnData &col_data_p, RowGroup &row_group_p, ColumnCheckpointState &state_p,
	                       ColumnCheckpointInfo &checkpoint_info);

public:
	void Checkpoint(const column_segment_vector_t &nodes);
	void FinalizeCheckpoint(column_segment_vector_t &&nodes);
	CompressionFunction &GetCompressionFunction(CompressionType type);

private:
	void ScanSegments(const column_segment_vector_t &nodes, const std::function<void(Vector &, idx_t)> &callback);
	unique_ptr<AnalyzeState> DetectBestCompressionMethod(const column_segment_vector_t &nodes, idx_t &compression_idx);
	void WriteToDisk(const column_segment_vector_t &nodes);
	bool HasChanges(const column_segment_vector_t &nodes);
	void WritePersistentSegments(column_segment_vector_t nodes);

private:
	ColumnData &col_data;
	RowGroup &row_group;
	ColumnCheckpointState &state;
	bool is_validity;
	bool has_changes;
	Vector intermediate;
	vector<optional_ptr<CompressionFunction>> compression_functions;
	ColumnCheckpointInfo &checkpoint_info;

	vector<unique_ptr<AnalyzeState>> analyze_states;
};

} // namespace duckdb
