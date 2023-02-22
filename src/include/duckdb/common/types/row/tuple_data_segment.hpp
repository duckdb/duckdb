//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class TupleDataAllocator;

struct TupleDataChunk {
public:
	TupleDataChunk(uint32_t row_block_index_p, idx_t row_block_offset_p, uint32_t heap_block_index_p,
	               idx_t heap_block_offset_p, idx_t last_heap_row_size_p, uint32_t count_p);

	//! Disable copy constructors
	TupleDataChunk(const TupleDataChunk &other) = delete;
	TupleDataChunk &operator=(const TupleDataChunk &) = delete;

	//! Enable move constructors
	TupleDataChunk(TupleDataChunk &&other) noexcept;
	TupleDataChunk &operator=(TupleDataChunk &&) noexcept;

public:
	//! Index/offset of the row block
	uint32_t row_block_index;
	idx_t row_block_offset;
	//! Index/offset of the heap block
	uint32_t heap_block_index;
	idx_t heap_block_offset;
	//! Size of the last heap row (for all but the last we can infer the size from the pointer difference)
	idx_t last_heap_row_size;
	//! Tuple count for this segment
	uint32_t count;
};

struct TupleDataSegment {
public:
	explicit TupleDataSegment(shared_ptr<TupleDataAllocator> allocator);

	//! Disable copy constructors
	TupleDataSegment(const TupleDataSegment &other) = delete;
	TupleDataSegment &operator=(const TupleDataSegment &) = delete;

	//! Enable move constructors
	TupleDataSegment(TupleDataSegment &&other) noexcept;
	TupleDataSegment &operator=(TupleDataSegment &&) noexcept;

public:
	//! The allocator for this segment
	shared_ptr<TupleDataAllocator> allocator;
	//! The chunks of this segment
	vector<TupleDataChunk> chunks;
};

} // namespace duckdb
