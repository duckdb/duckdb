//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/block.hpp"
#include "duckdb/storage/table/segment_tree.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"

namespace duckdb {
class CompressedSegment;
class BlockManager;
class ColumnSegment;
class ColumnData;
class DatabaseInstance;
class Transaction;
class BaseStatistics;
class UpdateSegment;
class TableFilter;
struct ColumnFetchState;
struct ColumnScanState;
struct ColumnAppendState;

enum class ColumnSegmentType : uint8_t { TRANSIENT, PERSISTENT };
//! TableFilter represents a filter pushed down into the table scan.

class ColumnSegment : public SegmentBase {
public:
	~ColumnSegment() override;

	//! The database instance
	DatabaseInstance &db;
	//! The type stored in the column
	LogicalType type;
	//! The size of the type
	idx_t type_size;
	//! The column segment type (transient or persistent)
	ColumnSegmentType segment_type;
	//! The statistics for the segment
	SegmentStatistics stats;
	//! The segment holding the data
	unique_ptr<CompressedSegment> data;

	static unique_ptr<ColumnSegment> CreatePersistentSegment(DatabaseInstance &db, block_id_t id, idx_t offset, const LogicalType &type_p, idx_t start, idx_t count, unique_ptr<BaseStatistics> statistics);
	static unique_ptr<ColumnSegment> CreateTransientSegment(DatabaseInstance &db, const LogicalType &type, idx_t start);

public:
	void InitializeScan(ColumnScanState &state);
	//! Scan one vector from this segment
	void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset,
	          bool entire_vector);
	//! Fetch a value of the specific row id and append it to the result
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx);

	//! Initialize an append of this transient segment
	void InitializeAppend(ColumnAppendState &state);
	//! Appends a (part of) vector to the transient segment, returns the amount of entries successfully appended
	idx_t Append(ColumnAppendState &state, VectorData &data, idx_t offset, idx_t count);
	//! Revert an append made to this transient segment
	void RevertAppend(idx_t start_row);

	block_id_t GetBlockId() {
		D_ASSERT(segment_type == ColumnSegmentType::PERSISTENT);
		return block_id;
	}

	idx_t GetBlockOffset() {
		D_ASSERT(segment_type == ColumnSegmentType::PERSISTENT);
		return offset;
	}

public:
	ColumnSegment(DatabaseInstance &db, LogicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count, unique_ptr<CompressedSegment> data);

	ColumnSegment(DatabaseInstance &db, LogicalType type, ColumnSegmentType segment_type, idx_t start, idx_t count,
	              unique_ptr<BaseStatistics> statistics, unique_ptr<CompressedSegment> data, block_id_t block_id, idx_t offset);

private:
	//! The block id that this segment relates to (persistent segment only)
	block_id_t block_id;
	//! The offset into the block (persistent segment only)
	idx_t offset;
};

} // namespace duckdb
