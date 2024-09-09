//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/partial_block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/storage/data_pointer.hpp"

namespace duckdb {
class DatabaseInstance;
class ClientContext;
class ColumnSegment;
class MetadataReader;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class TableCatalogEntry;
class ViewCatalogEntry;
class TypeCatalogEntry;

//! Regions that require zero-initialization to avoid leaking memory
struct UninitializedRegion {
	idx_t start;
	idx_t end;
};

//! The current state of a partial block
struct PartialBlockState {
	//! The block id of the partial block
	block_id_t block_id;
	//! The total bytes that we can assign to this block
	uint32_t block_size;
	//! Next allocation offset, and also the current allocation size
	uint32_t offset;
	//! The number of times that this block has been used for partial allocations
	uint32_t block_use_count;
};

struct PartialBlock {
	PartialBlock(PartialBlockState state, BlockManager &block_manager, const shared_ptr<BlockHandle> &block_handle);
	virtual ~PartialBlock() {
	}

	//! The current state of a partial block
	PartialBlockState state;
	//! All uninitialized regions on this block, we need to zero-initialize them when flushing
	vector<UninitializedRegion> uninitialized_regions;
	//! The block manager of the partial block manager
	BlockManager &block_manager;
	//! The block handle of the underlying block that this partial block writes to
	shared_ptr<BlockHandle> block_handle;

public:
	//! Add regions that need zero-initialization to avoid leaking memory
	void AddUninitializedRegion(const idx_t start, const idx_t end);
	//! Flush the block to disk and zero-initialize any free space and uninitialized regions
	virtual void Flush(const idx_t free_space_left) = 0;
	void FlushInternal(const idx_t free_space_left);
	virtual void Merge(PartialBlock &other, idx_t offset, idx_t other_size) = 0;
	virtual void Clear() = 0;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
};

struct PartialBlockAllocation {
	//! The BlockManager owning the block_id
	BlockManager *block_manager {nullptr};
	//! The number of assigned bytes to the caller
	uint32_t allocation_size;
	//! The current state of the partial block
	PartialBlockState state;
	//! Arbitrary state related to the partial block storage
	unique_ptr<PartialBlock> partial_block;
};

enum class PartialBlockType { FULL_CHECKPOINT, APPEND_TO_TABLE };

//! Enables sharing blocks across some scope. Scope is whatever we want to share
//! blocks across. It may be an entire checkpoint or just a single row group.
//! In any case, they must share a block manager.
class PartialBlockManager {
public:
	//! Max number of shared references to a block. No effective limit by default.
	static constexpr const idx_t DEFAULT_MAX_USE_COUNT = 1u << 20;
	//! No point letting map size grow unbounded. We'll drop blocks with the
	//! least free space first.
	static constexpr const idx_t MAX_BLOCK_MAP_SIZE = 1u << 31;

public:
	PartialBlockManager(BlockManager &block_manager, PartialBlockType partial_block_type,
	                    optional_idx max_partial_block_size = optional_idx(),
	                    uint32_t max_use_count = DEFAULT_MAX_USE_COUNT);
	virtual ~PartialBlockManager();

public:
	PartialBlockAllocation GetBlockAllocation(uint32_t segment_size);

	//! Register a partially filled block that is filled with "segment_size" entries
	void RegisterPartialBlock(PartialBlockAllocation allocation);

	//! Clear remaining blocks without writing them to disk
	void ClearBlocks();

	//! Rollback all data written by this partial block manager
	void Rollback();

	//! Merge this block manager into another one
	void Merge(PartialBlockManager &other);

	//! Flush any remaining partial blocks to disk
	void FlushPartialBlocks();

	unique_lock<mutex> GetLock() {
		return unique_lock<mutex>(partial_block_lock);
	}

	//! Returns a reference to the underlying block manager.
	BlockManager &GetBlockManager() const;

	//! Registers a block as "written" by this partial block manager
	void AddWrittenBlock(block_id_t block);

protected:
	BlockManager &block_manager;
	PartialBlockType partial_block_type;
	mutex partial_block_lock;
	//! A map of (available space -> PartialBlock) for partially filled blocks
	//! This is a multimap because there might be outstanding partial blocks with
	//! the same amount of left-over space
	multimap<idx_t, unique_ptr<PartialBlock>> partially_filled_blocks;
	//! The set of written blocks
	unordered_set<block_id_t> written_blocks;

	//! The maximum size (in bytes) at which a partial block will be considered a partial block
	uint32_t max_partial_block_size;
	uint32_t max_use_count;

protected:
	virtual void AllocateBlock(PartialBlockState &state, uint32_t segment_size);
	//! Try to obtain a partially filled block that can fit "segment_size" bytes
	//! If successful, returns true and returns the block_id and offset_in_block to write to
	//! Otherwise, returns false
	bool GetPartialBlock(idx_t segment_size, unique_ptr<PartialBlock> &state);

	bool HasBlockAllocation(uint32_t segment_size);
};

} // namespace duckdb
