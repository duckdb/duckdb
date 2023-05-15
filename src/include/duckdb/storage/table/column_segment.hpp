//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/column_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/block.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/table/segment_base.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {
class ColumnSegment;
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

class ColumnSegment : public SegmentBase<ColumnSegment> {
public:
	~ColumnSegment();

	//! The database instance
	DatabaseInstance &db;
	//! The type stored in the column
	LogicalType type;
	//! The size of the type
	idx_t type_size;
	//! The column segment type (transient or persistent)
	ColumnSegmentType segment_type;
	//! The compression function
	reference<CompressionFunction> function;
	//! The statistics for the segment
	SegmentStatistics stats;
	//! The block that this segment relates to
	shared_ptr<BlockHandle> block;

	static unique_ptr<ColumnSegment> CreatePersistentSegment(DatabaseInstance &db, BlockManager &block_manager,
	                                                         block_id_t id, idx_t offset, const LogicalType &type_p,
	                                                         idx_t start, idx_t count, CompressionType compression_type,
	                                                         BaseStatistics statistics);
	static unique_ptr<ColumnSegment> CreateTransientSegment(DatabaseInstance &db, const LogicalType &type, idx_t start,
	                                                        idx_t segment_size = Storage::BLOCK_SIZE);
	static unique_ptr<ColumnSegment> CreateSegment(ColumnSegment &other, idx_t start);

public:
	void InitializeScan(ColumnScanState &state);
	//! Scan one vector from this segment
	void Scan(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset, bool entire_vector);
	//! Fetch a value of the specific row id and append it to the result
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx);

	static idx_t FilterSelection(SelectionVector &sel, Vector &result, const TableFilter &filter,
	                             idx_t &approved_tuple_count, ValidityMask &mask);

	//! Skip a scan forward to the row_index specified in the scan state
	void Skip(ColumnScanState &state);

	// The maximum size of the buffer (in bytes)
	idx_t SegmentSize() const;
	//! Resize the block
	void Resize(idx_t segment_size);

	//! Initialize an append of this segment. Appends are only supported on transient segments.
	void InitializeAppend(ColumnAppendState &state);
	//! Appends a (part of) vector to the segment, returns the amount of entries successfully appended
	idx_t Append(ColumnAppendState &state, UnifiedVectorFormat &data, idx_t offset, idx_t count);
	//! Finalize the segment for appending - no more appends can follow on this segment
	//! The segment should be compacted as much as possible
	//! Returns the number of bytes occupied within the segment
	idx_t FinalizeAppend(ColumnAppendState &state);
	//! Revert an append made to this segment
	void RevertAppend(idx_t start_row);

	//! Convert a transient in-memory segment into a persistent segment blocked by an on-disk block.
	//! Only used during checkpointing.
	void ConvertToPersistent(optional_ptr<BlockManager> block_manager, block_id_t block_id);
	//! Updates pointers to refer to the given block and offset. This is only used
	//! when sharing a block among segments. This is invoked only AFTER the block is written.
	void MarkAsPersistent(shared_ptr<BlockHandle> block, uint32_t offset_in_block);

	block_id_t GetBlockId() {
		D_ASSERT(segment_type == ColumnSegmentType::PERSISTENT);
		return block_id;
	}

	BlockManager &GetBlockManager() const {
		return block->block_manager;
	}

	idx_t GetBlockOffset() {
		D_ASSERT(segment_type == ColumnSegmentType::PERSISTENT || offset == 0);
		return offset;
	}

	idx_t GetRelativeIndex(idx_t row_index) {
		D_ASSERT(row_index >= this->start);
		D_ASSERT(row_index <= this->start + this->count);
		return row_index - this->start;
	}

	CompressedSegmentState *GetSegmentState() {
		return segment_state.get();
	}

public:
	ColumnSegment(DatabaseInstance &db, shared_ptr<BlockHandle> block, LogicalType type, ColumnSegmentType segment_type,
	              idx_t start, idx_t count, CompressionFunction &function, BaseStatistics statistics,
	              block_id_t block_id, idx_t offset, idx_t segment_size);
	ColumnSegment(ColumnSegment &other, idx_t start);

private:
	void Scan(ColumnScanState &state, idx_t scan_count, Vector &result);
	void ScanPartial(ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset);

private:
	//! The block id that this segment relates to (persistent segment only)
	block_id_t block_id;
	//! The offset into the block (persistent segment only)
	idx_t offset;
	//! The allocated segment size
	idx_t segment_size;
	//! Storage associated with the compressed segment
	unique_ptr<CompressedSegmentState> segment_state;
};

} // namespace duckdb
