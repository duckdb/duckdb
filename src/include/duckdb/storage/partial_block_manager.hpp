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
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/storage/data_pointer.hpp"

namespace duckdb {
class DatabaseInstance;
class ClientContext;
class ColumnSegment;
class MetaBlockReader;
class SchemaCatalogEntry;
class SequenceCatalogEntry;
class TableCatalogEntry;
class ViewCatalogEntry;
class TypeCatalogEntry;

struct PartialBlockState {
	block_id_t block_id;
	//! How big is the block we're writing to. (Total bytes to assign).
	uint32_t block_size;
	//! How many bytes of the allocation are used. (offset_in_block of next allocation)
	uint32_t offset_in_block;
	//! How many times has the block been used?
	uint32_t block_use_count;
};

struct PartialBlock {
	explicit PartialBlock(PartialBlockState state) : state(std::move(state)) {
	}
	virtual ~PartialBlock() {
	}

	PartialBlockState state;

public:
	virtual void AddUninitializedRegion(idx_t start, idx_t end) = 0;
	virtual void Flush(idx_t free_space_left) = 0;
	virtual void Clear() {
	}
	virtual void Merge(PartialBlock &other, idx_t offset, idx_t other_size);

public:
	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
};

struct PartialBlockAllocation {
	// BlockManager owning the block_id
	BlockManager *block_manager {nullptr};
	//! How many bytes assigned to the caller?
	uint32_t allocation_size;
	//! State of assigned block.
	PartialBlockState state;
	//! Arbitrary state related to partial block storage.
	unique_ptr<PartialBlock> partial_block;
};

enum class CheckpointType { FULL_CHECKPOINT, APPEND_TO_TABLE };

//! Enables sharing blocks across some scope. Scope is whatever we want to share
//! blocks across. It may be an entire checkpoint or just a single row group.
//! In any case, they must share a block manager.
class PartialBlockManager {
public:
	// 20% free / 80% utilization
	static constexpr const idx_t DEFAULT_MAX_PARTIAL_BLOCK_SIZE = Storage::BLOCK_SIZE / 5 * 4;
	// Max number of shared references to a block. No effective limit by default.
	static constexpr const idx_t DEFAULT_MAX_USE_COUNT = 1u << 20;
	// No point letting map size grow unbounded. We'll drop blocks with the
	// least free space first.
	static constexpr const idx_t MAX_BLOCK_MAP_SIZE = 1u << 31;

public:
	PartialBlockManager(BlockManager &block_manager, CheckpointType checkpoint_type,
	                    uint32_t max_partial_block_size = DEFAULT_MAX_PARTIAL_BLOCK_SIZE,
	                    uint32_t max_use_count = DEFAULT_MAX_USE_COUNT);
	virtual ~PartialBlockManager();

public:
	//! Flush any remaining partial blocks to disk
	void FlushPartialBlocks();

	PartialBlockAllocation GetBlockAllocation(uint32_t segment_size);

	virtual void AllocateBlock(PartialBlockState &state, uint32_t segment_size);

	void Merge(PartialBlockManager &other);
	//! Register a partially filled block that is filled with "segment_size" entries
	void RegisterPartialBlock(PartialBlockAllocation &&allocation);

	//! Clear remaining blocks without writing them to disk
	void ClearBlocks();

	//! Rollback all data written by this partial block manager
	void Rollback();

protected:
	BlockManager &block_manager;
	CheckpointType checkpoint_type;
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
	//! Try to obtain a partially filled block that can fit "segment_size" bytes
	//! If successful, returns true and returns the block_id and offset_in_block to write to
	//! Otherwise, returns false
	bool GetPartialBlock(idx_t segment_size, unique_ptr<PartialBlock> &state);

	bool HasBlockAllocation(uint32_t segment_size);
	void AddWrittenBlock(block_id_t block);
};

} // namespace duckdb
