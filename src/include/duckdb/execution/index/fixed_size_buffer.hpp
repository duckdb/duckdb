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
//! in memory, and it only serializes dirty and non-written buffers to disk during serialization.
class FixedSizeBuffer {
	friend class FixedSizeAllocator;
	friend class SegmentHandle;

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

	~FixedSizeBuffer();

private:
	//! Returns a pointer to the buffer in memory, and calls Deserialize, if the buffer is not in memory.
	//! DEPRECATED. Use segment handles.
	data_ptr_t GetDeprecated(const bool dirty_p = true) {
		lock_guard<mutex> l(lock);
		if (!InMemory()) {
			LoadFromDisk();
		}
		if (dirty_p) {
			dirty = dirty_p;
		}
		return buffer_handle.Ptr();
	}

	//! Returns true, if the buffer is in-memory
	bool InMemory() const {
		return buffer_handle.IsValid();
	}

	//! Returns true, if the block is on-disk
	bool OnDisk() const {
		return block_pointer.IsValid();
	}

	//! Serializes a buffer, if dirty or not on disk.
	void Serialize(PartialBlockManager &partial_block_manager, const idx_t available_segments, const idx_t segment_size,
	               const idx_t bitmask_offset);

	//! Load a buffer from disk, if not in memory.
	void LoadFromDisk();
	//! Returns the first free offset in a bitmask
	uint32_t GetOffset(const idx_t bitmask_count, const idx_t available_segments);
	//! Sets the allocation size, if dirty
	void SetAllocationSize(const idx_t available_segments, const idx_t segment_size, const idx_t bitmask_offset);

private:
	//! Block manager of the database instance
	BlockManager &block_manager;

	//! The number of active segments.
	atomic<idx_t> readers;

	//! The number of allocated segments
	idx_t segment_count;
	//! The size of allocated memory in this buffer (necessary for copying while pinning)
	idx_t allocation_size;

	//! True: the in-memory buffer is no longer consistent with its optional copy on disk.
	bool dirty;
	//! True: can be vacuumed after the vacuum operation.
	bool vacuum;
	//! True: has been loaded from disk.
	bool loaded;

	//! Partial block id and offset
	BlockPointer block_pointer;
	//! The buffer handle of the in-memory buffer
	BufferHandle buffer_handle;
	//! The block handle of the on-disk buffer
	shared_ptr<BlockHandle> block_handle;
	//! The lock for this fixed size buffer handle
	mutex lock;
};

class SegmentHandle {
public:
	SegmentHandle() = delete;
	SegmentHandle(FixedSizeBuffer &buffer_p, const idx_t offset) : buffer_ptr(buffer_p) {
		lock_guard<mutex> l(buffer_ptr->lock);

		if (!buffer_ptr->InMemory() && !buffer_ptr->loaded) {
			buffer_ptr->LoadFromDisk();
		}
		if (!buffer_ptr->InMemory() && buffer_ptr->loaded) {
			buffer_ptr->block_manager.buffer_manager.Pin(buffer_ptr->block_handle);
		}

		ptr = buffer_ptr->buffer_handle.Ptr() + offset;
		buffer_ptr->readers++;
	}

	~SegmentHandle() {
		if (!buffer_ptr) {
			return;
		}
		buffer_ptr->readers--;
		buffer_ptr = nullptr;
		ptr = nullptr;

		// FIXME: Enable unpinning buffers with zero readers while preventing oscillation.
		// FIXME: loaded must be set to true.
	}

	SegmentHandle(const SegmentHandle &) = delete;
	SegmentHandle &operator=(const SegmentHandle &) = delete;

	SegmentHandle(SegmentHandle &&other) noexcept : buffer_ptr(other.buffer_ptr), ptr(other.ptr) {
		other.buffer_ptr = nullptr;
		other.ptr = nullptr;
	}
	SegmentHandle &operator=(SegmentHandle &&other) noexcept {
		if (this != &other) {
			buffer_ptr = other.buffer_ptr;
			ptr = other.ptr;
			other.buffer_ptr = nullptr;
			other.ptr = nullptr;
		}
		return *this;
	}

public:
	template <class T>
	const T &GetRef() const {
		return *reinterpret_cast<const T *>(ptr);
	}

	template <class T>
	T &GetRef() {
		return *reinterpret_cast<T *>(ptr);
	}

	template <class T = data_t>
	T *GetPtr() {
		return reinterpret_cast<T *>(ptr);
	}

	template <class T = data_t>
	const T *GetPtr() const {
		return reinterpret_cast<T *>(ptr);
	}

	void MarkModified() {
		lock_guard<mutex> l(buffer_ptr->lock);
		buffer_ptr->dirty = true;
	}

private:
	optional_ptr<FixedSizeBuffer> buffer_ptr;
	data_ptr_t ptr;
};

} // namespace duckdb
