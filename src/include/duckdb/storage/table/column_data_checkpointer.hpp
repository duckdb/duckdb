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

class ColumnDataCheckpointer {
public:
	ColumnDataCheckpointer(ColumnData &col_data_p, RowGroup &row_group_p, ColumnCheckpointState &state_p,
	                       ColumnCheckpointInfo &checkpoint_info);

public:
	DatabaseInstance &GetDatabase();
	const LogicalType &GetType() const;
	ColumnData &GetColumnData();
	RowGroup &GetRowGroup();
	ColumnCheckpointState &GetCheckpointState();

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
};

} // namespace duckdb
