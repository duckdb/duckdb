//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/numeric_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/uncompressed_segment.hpp"

namespace duckdb {

struct StringBlock {
	block_id_t block_id;
	index_t offset;
	index_t size;
	unique_ptr<StringBlock> next;
};

class StringSegment : public UncompressedSegment {
public:
	StringSegment(ColumnData &column_data, BufferManager &manager);

	//! The current dictionary offset
	index_t dictionary_offset;
	//! The main string block
	unique_ptr<StringBlock> head;
	//! Whether or not the block has any big strings
	bool has_big_strings;
public:
	void InitializeScan(TransientScanState &state) override;
	//! Fetch the vector at index "vector_index" from the uncompressed segment, storing it in the result vector
	void Scan(Transaction &transaction, TransientScanState &state, index_t vector_index, Vector &result) override;
	//! Fetch the vector at index "vector_index" from the uncompressed segment, throwing an exception if there are any outstanding updates
	void IndexScan(TransientScanState &state, index_t vector_index, Vector &result) override;

	//! Fetch a single vector from the base table
	void Fetch(index_t vector_index, Vector &result) override;
	//! Fetch a single value and append it to the vector
	void Fetch(Transaction &transaction, row_t row_id, Vector &result) override;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is full.
	index_t Append(SegmentStatistics &stats, TransientAppendState &state, Vector &data, index_t offset, index_t count) override;

	//! Rollback a previous update
	void RollbackUpdate(UpdateInfo *info) override;
	//! Cleanup an update, removing it from the version chain. This should only be called if an exclusive lock is held on the segment
	void CleanupUpdate(UpdateInfo *info) override;
protected:
	void Update(SegmentStatistics &stats, Transaction &transaction, Vector &update, row_t *ids, index_t vector_index, index_t vector_offset, UpdateInfo *node) override;
private:
	void AppendData(SegmentStatistics &stats, data_ptr_t target, data_ptr_t end, index_t target_offset, Vector &source, index_t offset, index_t count);

	void FetchBaseData(TransientScanState &state, data_ptr_t base_data, index_t vector_index, Vector &result, index_t count);

	void WriteString(const char *str, index_t string_length, block_id_t &result_block, int32_t &result_offset);

	void WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset);
	void ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset);
private:
	//! The max string size that is allowed within a block. Strings bigger than this will be labeled as a BIG STRING and offloaded to the overflow blocks.
	static constexpr uint16_t STRING_BLOCK_LIMIT = 4096;
	//! Marker used in length field to indicate the presence of a big string
	static constexpr uint16_t BIG_STRING_MARKER = (uint16_t) -1;
	//! The marker size of the big string
	static constexpr index_t BIG_STRING_MARKER_SIZE = sizeof(block_id_t) + sizeof(int32_t) + sizeof(uint16_t);
};

}
