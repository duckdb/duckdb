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

struct TupleDataChunkPart {
public:
	TupleDataChunkPart(uint32_t row_block_index_p, uint32_t row_block_offset_p, uint32_t heap_block_index_p,
	                   uint32_t heap_block_offset_p, data_ptr_t base_heap_ptr, idx_t last_heap_row_size_p,
	                   uint32_t count_p);

	//! Disable copy constructors
	TupleDataChunkPart(const TupleDataChunkPart &other) = delete;
	TupleDataChunkPart &operator=(const TupleDataChunkPart &) = delete;

	//! Enable move constructors
	TupleDataChunkPart(TupleDataChunkPart &&other) noexcept;
	TupleDataChunkPart &operator=(TupleDataChunkPart &&) noexcept;

public:
	//! Index/offset of the row block
	uint32_t row_block_index;
	uint32_t row_block_offset;
	//! Pointer/index/offset of the heap block
	uint32_t heap_block_index;
	uint32_t heap_block_offset;
	data_ptr_t base_heap_ptr;
	//! Size of the last heap row (for all but the last we can infer the size from the pointer difference)
	uint32_t last_heap_size;
	//! Tuple count for this chunk part
	uint32_t count;
};

struct TupleDataChunk {
public:
	TupleDataChunk();

	//! Disable copy constructors
	TupleDataChunk(const TupleDataChunk &other) = delete;
	TupleDataChunk &operator=(const TupleDataChunk &) = delete;

	//! Enable move constructors
	TupleDataChunk(TupleDataChunk &&other) noexcept;
	TupleDataChunk &operator=(TupleDataChunk &&) noexcept;

	//! Add a part to this chunk
	void AddPart(TupleDataChunkPart &&part);
	//! Verify counts of the parts in this chunk
	void Verify() const;

public:
	//! The parts of this chunk
	vector<TupleDataChunkPart> parts;
	//! The row block ids referenced by the chunk
	unordered_set<uint32_t> row_block_ids;
	//! The heap block ids referenced by the chunk
	unordered_set<uint32_t> heap_block_ids;
	//! Tuple count for this chunk
	idx_t count;
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

	//! The number of chunks in this segment
	idx_t ChunkCount() const;
	//! Verify counts of the chunks in this segment
	void Verify() const;

public:
	//! The allocator for this segment
	shared_ptr<TupleDataAllocator> allocator;
	//! The chunks of this segment
	vector<TupleDataChunk> chunks;
	//! The tuple count of this segment
	idx_t count;
	//! TODO
	vector<BufferHandle> pinned_handles;
};

} // namespace duckdb
