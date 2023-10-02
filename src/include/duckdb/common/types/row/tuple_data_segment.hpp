//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/perfect_map_set.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

class TupleDataAllocator;
class TupleDataLayout;

struct TupleDataChunkPart {
public:
	TupleDataChunkPart(mutex &lock);

	//! Disable copy constructors
	TupleDataChunkPart(const TupleDataChunkPart &other) = delete;
	TupleDataChunkPart &operator=(const TupleDataChunkPart &) = delete;

	//! Enable move constructors
	TupleDataChunkPart(TupleDataChunkPart &&other) noexcept;
	TupleDataChunkPart &operator=(TupleDataChunkPart &&) noexcept;

	static constexpr const uint32_t INVALID_INDEX = (uint32_t)-1;

public:
	//! Index/offset of the row block
	uint32_t row_block_index;
	uint32_t row_block_offset;
	//! Pointer/index/offset of the heap block
	uint32_t heap_block_index;
	uint32_t heap_block_offset;
	data_ptr_t base_heap_ptr;
	//! Total heap size for this chunk part
	uint32_t total_heap_size;
	//! Tuple count for this chunk part
	uint32_t count;
	//! Lock for recomputing heap pointers (owned by TupleDataChunk)
	reference<mutex> lock;
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
	void AddPart(TupleDataChunkPart &&part, const TupleDataLayout &layout);
	//! Tries to merge the last chunk part into the second-to-last one
	void MergeLastChunkPart(const TupleDataLayout &layout);
	//! Verify counts of the parts in this chunk
	void Verify() const;

public:
	//! The parts of this chunk
	unsafe_vector<TupleDataChunkPart> parts;
	//! The row block ids referenced by the chunk
	perfect_set_t row_block_ids;
	//! The heap block ids referenced by the chunk
	perfect_set_t heap_block_ids;
	//! Tuple count for this chunk
	idx_t count;
	//! Lock for recomputing heap pointers
	unsafe_unique_ptr<mutex> lock;
};

struct TupleDataSegment {
public:
	explicit TupleDataSegment(shared_ptr<TupleDataAllocator> allocator);

	~TupleDataSegment();

	//! Disable copy constructors
	TupleDataSegment(const TupleDataSegment &other) = delete;
	TupleDataSegment &operator=(const TupleDataSegment &) = delete;

	//! Enable move constructors
	TupleDataSegment(TupleDataSegment &&other) noexcept;
	TupleDataSegment &operator=(TupleDataSegment &&) noexcept;

	//! The number of chunks in this segment
	idx_t ChunkCount() const;
	//! The size (in bytes) of this segment
	idx_t SizeInBytes() const;
	//! Unpins all held pins
	void Unpin();

	//! Verify counts of the chunks in this segment
	void Verify() const;
	//! Verify that all blocks in this segment are pinned
	void VerifyEverythingPinned() const;

public:
	//! The allocator for this segment
	shared_ptr<TupleDataAllocator> allocator;
	//! The chunks of this segment
	unsafe_vector<TupleDataChunk> chunks;
	//! The tuple count of this segment
	idx_t count;
	//! The data size of this segment
	idx_t data_size;

	//! Lock for modifying pinned_handles
	mutex pinned_handles_lock;
	//! Where handles to row blocks will be stored with TupleDataPinProperties::KEEP_EVERYTHING_PINNED
	unsafe_vector<BufferHandle> pinned_row_handles;
	//! Where handles to heap blocks will be stored with TupleDataPinProperties::KEEP_EVERYTHING_PINNED
	unsafe_vector<BufferHandle> pinned_heap_handles;
};

} // namespace duckdb
