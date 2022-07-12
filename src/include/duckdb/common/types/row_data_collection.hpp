//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row_data_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

struct RowDataBlock {
	RowDataBlock(BufferManager &buffer_manager, idx_t capacity, idx_t entry_size)
	    : capacity(capacity), entry_size(entry_size), count(0), byte_offset(0) {
		block = buffer_manager.RegisterMemory(capacity * entry_size, false);
	}
	//! The buffer block handle
	shared_ptr<BlockHandle> block;
	//! Capacity (number of entries) and entry size that fit in this block
	idx_t capacity;
	const idx_t entry_size;
	//! Number of entries currently in this block
	idx_t count;
	//! Write offset (if variable size entries)
	idx_t byte_offset;
};

struct BlockAppendEntry {
	BlockAppendEntry(data_ptr_t baseptr, idx_t count) : baseptr(baseptr), count(count) {
	}
	data_ptr_t baseptr;
	idx_t count;
};

class RowDataCollection {
public:
	RowDataCollection(BufferManager &buffer_manager, idx_t block_capacity, idx_t entry_size, bool keep_pinned = false);

	unique_ptr<RowDataCollection> CloneEmpty(bool keep_pinned = false) const {
		return make_unique<RowDataCollection>(buffer_manager, block_capacity, entry_size, keep_pinned);
	}

	//! BufferManager
	BufferManager &buffer_manager;
	//! The total number of stored entries
	idx_t count;
	//! The number of entries per block
	idx_t block_capacity;
	//! Size of entries in the blocks
	idx_t entry_size;
	//! The blocks holding the main data
	vector<RowDataBlock> blocks;
	//! The blocks that this collection currently has pinned
	vector<BufferHandle> pinned_blocks;

public:
	idx_t AppendToBlock(RowDataBlock &block, BufferHandle &handle, vector<BlockAppendEntry> &append_entries,
	                    idx_t remaining, idx_t entry_sizes[]);
	vector<BufferHandle> Build(idx_t added_count, data_ptr_t key_locations[], idx_t entry_sizes[],
	                           const SelectionVector *sel = FlatVector::IncrementalSelectionVector());

	void Merge(RowDataCollection &other);

	void Clear() {
		blocks.clear();
		pinned_blocks.clear();
		count = 0;
	}

	//! The size (in bytes) of this RowDataCollection if it were stored in a single block
	idx_t SizeInBytes() const {
		idx_t bytes = 0;
		if (entry_size == 1) {
			for (auto &block : blocks) {
				bytes += block.byte_offset;
			}
		} else {
			bytes = count * entry_size;
		}
		return MaxValue(bytes, (idx_t)Storage::BLOCK_SIZE);
	}

private:
	mutex rdc_lock;

	//! Whether the blocks should stay pinned (necessary for e.g. a heap)
	const bool keep_pinned;
};

} // namespace duckdb
