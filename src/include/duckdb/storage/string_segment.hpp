//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/string_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/uncompressed_segment.hpp"

namespace duckdb {
class StorageManager;

class OverflowStringWriter {
public:
	virtual ~OverflowStringWriter() {
	}

	virtual void WriteString(string_t string, block_id_t &result_block, int32_t &result_offset) = 0;
};

struct StringBlock {
	shared_ptr<BlockHandle> block;
	idx_t offset;
	idx_t size;
	unique_ptr<StringBlock> next;
};

struct string_location_t {
	string_location_t(block_id_t block_id, int32_t offset) : block_id(block_id), offset(offset) {
	}
	string_location_t() {
	}
	bool IsValid() {
		return offset < Storage::BLOCK_SIZE && (block_id == INVALID_BLOCK || block_id >= MAXIMUM_BLOCK);
	}
	block_id_t block_id;
	int32_t offset;
};

class StringSegment : public UncompressedSegment {
public:
	StringSegment(DatabaseInstance &db, idx_t row_start, block_id_t block_id = INVALID_BLOCK);
	~StringSegment() override;

	//! The string block holding strings that do not fit in the main block
	//! FIXME: this should be replaced by a heap that also allows freeing of unused strings
	unique_ptr<StringBlock> head;
	//! Overflow string writer (if any), if not set overflow strings will be written to memory blocks
	unique_ptr<OverflowStringWriter> overflow_writer;
	//! Map of block id to string block
	unordered_map<block_id_t, StringBlock *> overflow_blocks;

public:
	void InitializeScan(ColumnScanState &state) override;

	//! Scans a vector of "scan_count" entries starting at position "start"
	//! Store it in result with offset "result_offset"
	void Scan(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result) override;
	void ScanPartial(ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) override;

	//! Fetch a single value and append it to the vector
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;

	//! Append a part of a vector to the uncompressed segment with the given append state, updating the provided stats
	//! in the process. Returns the amount of tuples appended. If this is less than `count`, the uncompressed segment is
	//! full.
	idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) override;

private:
	string_location_t FetchStringLocation(data_ptr_t baseptr, int32_t dict_offset);
	string_t FetchString(Vector &result, data_ptr_t baseptr, string_location_t location);
	//! Fetch a single string from the dictionary and returns it, potentially pins a buffer manager page and adds it to
	//! the set of pinned pages
	string_t FetchStringFromDict(Vector &result, data_ptr_t baseptr, int32_t dict_offset);

	void WriteString(string_t string, block_id_t &result_block, int32_t &result_offset);
	string_t ReadString(Vector &result, block_id_t block, int32_t offset);
	string_t ReadString(data_ptr_t target, int32_t offset);

	void WriteStringMemory(string_t string, block_id_t &result_block, int32_t &result_offset);

	void WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset);
	void ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset);

	//! The amount of bytes remaining to store in the block
	idx_t RemainingSpace(BufferHandle &handle);

	void ReadString(string_t *result_data, Vector &result, data_ptr_t baseptr, int32_t *dict_offset, idx_t src_idx,
	                idx_t res_idx, idx_t &update_idx, size_t vector_index);

	void SetDictionaryOffset(BufferHandle &handle, idx_t offset);
	idx_t GetDictionaryOffset(BufferHandle &handle);

private:
	//! The max string size that is allowed within a block. Strings bigger than this will be labeled as a BIG STRING and
	//! offloaded to the overflow blocks.
	static constexpr uint16_t STRING_BLOCK_LIMIT = 4096;
	//! Marker used in length field to indicate the presence of a big string
	static constexpr uint16_t BIG_STRING_MARKER = (uint16_t)-1;
	//! Base size of big string marker (block id + offset)
	static constexpr idx_t BIG_STRING_MARKER_BASE_SIZE = sizeof(block_id_t) + sizeof(int32_t);
	//! The marker size of the big string
	static constexpr idx_t BIG_STRING_MARKER_SIZE = BIG_STRING_MARKER_BASE_SIZE + sizeof(uint16_t);
};

} // namespace duckdb
