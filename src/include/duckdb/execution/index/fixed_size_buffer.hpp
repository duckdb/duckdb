//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/fixed_size_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/block_manager.hpp"

namespace duckdb {

class FixedSizeAllocator;
class MetadataWriter;

struct PartialBlockForIndex : public PartialBlock {
public:
	explicit PartialBlockForIndex(BlockManager &block_manager, PartialBlockState state,
	                              const shared_ptr<BlockHandle> &block_handle)
	    : PartialBlock(state), block_manager(block_manager), block_handle(block_handle) {
	}
	~PartialBlockForIndex() override {};

	BlockManager &block_manager;
	shared_ptr<BlockHandle> block_handle;

public:
	inline bool IsFlushed() {
		return block_handle == nullptr;
	};
	void AddUninitializedRegion(idx_t start, idx_t end) override;
	void Flush(idx_t free_space_left) override;
	inline void Clear() override {
		block_handle.reset();
	};
	void Merge(PartialBlock &other, idx_t offset, idx_t other_size) override;
};

//! A fixed-size buffer holds fixed-size segments of data. It lazily deserializes a buffer, if on-disk and not
//! yet in memory, and it only serializes dirty and non-written buffers to disk during
//! serialization.
class FixedSizeBuffer {
public:
	//! Constructor for a new in-memory buffer
	explicit FixedSizeBuffer(BlockManager &block_manager);
	//! Constructor for deserializing buffer metadata from disk
	FixedSizeBuffer(BlockManager &block_manager, const idx_t segment_count, const idx_t allocation_size,
	                const BlockPointer &block_ptr);

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
	void Serialize(PartialBlockManager &partial_block_manager, const idx_t allocation_size_p);
	//! Pin a buffer (if not in-memory)
	void Pin();

private:
	//! The buffer handle of the in-memory buffer
	BufferHandle buffer_handle;
	//! The block handle of the on-disk buffer
	shared_ptr<BlockHandle> block_handle;
};

} // namespace duckdb
