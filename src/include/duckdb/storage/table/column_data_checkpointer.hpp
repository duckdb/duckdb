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

namespace duckdb {

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

	void Checkpoint(unique_ptr<SegmentBase> segment);
	void DetectBestCompressionMethod(idx_t &compression_idx, bool is_validity);
	static void ForceCompression(vector<CompressionFunction *> &compression_functions,
	                             CompressionType compression_type);

private:
	void ScanSegments(const std::function<void(Vector &, idx_t)> &callback);
	void WriteToDisk();
	bool HasChanges();
	void WritePersistentSegments();

private:
	ColumnData &col_data;
	RowGroup &row_group;
	ColumnCheckpointState &state;
	bool is_validity;
	Vector intermediate;
	unique_ptr<SegmentBase> owned_segment;
	vector<CompressionFunction *> compression_functions;
	ColumnCheckpointInfo &checkpoint_info;
};

} // namespace duckdb
