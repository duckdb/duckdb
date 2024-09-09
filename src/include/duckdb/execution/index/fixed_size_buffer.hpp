//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/fixed_size_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/block_manager.hpp"

namespace duckdb {

class FixedSizeAllocator;
class MetadataWriter;

struct PartialBlockForIndex : public PartialBlock {
public:
	PartialBlockForIndex(PartialBlockState state, BlockManager &block_manager,
	                     const shared_ptr<BlockHandle> &block_handle);
	~PartialBlockForIndex() override {};

public:
	void Flush(const idx_t free_space_left) override;
	void Clear() override;
	void Merge(PartialBlock &other, idx_t offset, idx_t other_size) override;
};

//! A fixed-size buffer holds fixed-size segments of data. It lazily deserializes a buffer, if on-disk and not
//! yet in memory, and it only serializes dirty and non-written buffers to disk during
//! serialization.
class FixedSizeBuffer {
public:
	//! Constants for fast offset calculations in the bitmask
	static constexpr idx_t BASE[] = {0x00000000FFFFFFFF, 0x0000FFFF, 0x00FF, 0x0F, 0x3, 0x1};
	static constexpr uint8_t SHIFT[] = {32, 16, 8, 4, 2, 1};

public:
	//! Constructor for a new in-memory buffer
	explicit FixedSizeBuffer(BlockManager &block_manager);
	//! Constructor for deserializing buffer metadata from disk
	FixedSizeBuffer(BlockManager &block_manager, const idx_t segment_count, const idx_t allocation_size,
	                const BlockPointer &block_pointer);

	//! Block manager of the database instance
	BlockManager &block_manager;

	//! The number of allocated segments
	idx_t segment_count;
	//! The size of allocated memory in this buffer (necessary for copying while pinning)
	idx_t allocation_size;

	//! True: the in-memory buffer is no longer consistent with a (possibly existing) copy on disk
	bool dirty;
	//! True: can be vacuumed after the vacuum operation
	bool vacuum;

	//! Partial block id and offset
	BlockPointer block_pointer;

public:
	//! Returns true, if the buffer is in-memory
	inline bool InMemory() const {
		return buffer_handle.IsValid();
	}
	//! Returns true, if the block is on-disk
	inline bool OnDisk() const {
		return block_pointer.IsValid();
	}
	//! Returns a pointer to the buffer in memory, and calls Deserialize, if the buffer is not in memory
	inline data_ptr_t Get(const bool dirty_p = true) {
		if (!InMemory()) {
			Pin();
		}
		if (dirty_p) {
			dirty = dirty_p;
		}
		return buffer_handle.Ptr();
	}
	//! Destroys the in-memory buffer and the on-disk block
	void Destroy();
	//! Serializes a buffer (if dirty or not on disk)
	void Serialize(PartialBlockManager &partial_block_manager, const idx_t available_segments, const idx_t segment_size,
	               const idx_t bitmask_offset);
	//! Pin a buffer (if not in-memory)
	void Pin();
	//! Returns the first free offset in a bitmask
	uint32_t GetOffset(const idx_t bitmask_count);
	//! Sets the allocation size, if dirty
	void SetAllocationSize(const idx_t available_segments, const idx_t segment_size, const idx_t bitmask_offset);

private:
	//! The buffer handle of the in-memory buffer
	BufferHandle buffer_handle;
	//! The block handle of the on-disk buffer
	shared_ptr<BlockHandle> block_handle;

private:
	//! Sets all uninitialized regions of a buffer in the respective partial block allocation
	void SetUninitializedRegions(PartialBlockForIndex &p_block_for_index, const idx_t segment_size, const idx_t offset,
	                             const idx_t bitmask_offset);
};

} // namespace duckdb
