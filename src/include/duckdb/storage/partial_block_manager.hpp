//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/partial_block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include <cstdint>

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

//! Interface required for packing data into partial blocks, implementing
//! classes can be managed through the partial block manager.
class BlockPackable {
public:
	virtual ~BlockPackable() = default;

public:
	virtual void Finalize(shared_ptr<BlockHandle> block_handle_p, uint32_t offset) = 0;
};

struct PartialBlock {
	PartialBlock(BlockManager &block_manager, block_id_t block_id);
	virtual ~PartialBlock() {
	}

	//! All uninitialized regions on this block, we need to zero-initialize them when flushing
	vector<UninitializedRegion> uninitialized_regions;
	//! The block manager of the partial block manager
	BlockManager &block_manager;
	//! The block handle of the underlying block that this partial block writes to
	shared_ptr<BlockHandle> block_handle;

public:
	idx_t AvailableSpace();
	uint32_t UseCount();
	//! Add regions that need zero-initialization to avoid leaking memory
	void AddUninitializedRegion(const idx_t start, const idx_t end);
	//! Register the object at the current allocation offset.
	void AddPackable(BlockPackable &packable);

	shared_ptr<BlockHandle> ReuseBuffer(BufferHandle &source);
	BlockPointer Store(BlockPackable &packable, BufferHandle source, uint32_t byte_size);
	//! Flush the block to disk and zero-initialize any free space and uninitialized regions
	virtual void Flush(QueryContext context);
	void FlushInternal(const idx_t free_space_left);
	//! Merge bytes and segment references before either block has a persistent block ID.
	virtual void Merge(PartialBlock &other);
	virtual void Clear();

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

protected:
	//! Install the final buffer and offsets before releasing the partial block.
	void FinalizePackables();

private:
	//! Type-erased buffers that will be packed in the current block.
	vector<std::pair<reference<BlockPackable>, uint32_t>> packables;
	//! The block id of the partial block
	block_id_t block_id;
	//! Next allocation offset, and also the current allocation size
	uint32_t offset = 0;
	//! The number of references to this block from partial allocations
	uint32_t block_use_count = 0;
};

enum class PartialBlockType { FULL_CHECKPOINT, APPEND_TO_TABLE, IN_MEMORY_CHECKPOINT };

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
	PartialBlockManager(QueryContext context, BlockManager &block_manager, PartialBlockType partial_block_type,
	                    optional_idx max_partial_block_size = optional_idx(),
	                    uint32_t max_use_count = DEFAULT_MAX_USE_COUNT);
	virtual ~PartialBlockManager();

public:
	//! Clear remaining blocks without writing them to disk
	void ClearBlocks();

	//! Rollback all data written by this partial block manager
	void Rollback();

	//! Merge this block manager into another one
	void Merge(PartialBlockManager &other);

	//! Flush any remaining partial blocks to disk
	void FlushPartialBlocks();

	unique_ptr<PartialBlock> TakePartialBlock(uint32_t byte_size);

	unique_ptr<PartialBlock> CreatePartialBlock();

	unique_ptr<PartialBlock> AcquirePartialBlock(uint32_t byte_size);

	void Retain(unique_ptr<PartialBlock> partial_block);

	unique_ptr<PartialBlock> PruneFreeList();

	void RetainOrFlush(unique_ptr<PartialBlock> partial_block);

	optional<BlockPointer> Write(BlockPackable &packable, BufferHandle source, uint32_t byte_size);

	unique_lock<mutex> GetLock() {
		return unique_lock<mutex>(partial_block_lock);
	}
	block_id_t GetFreeBlockId();

	//! Returns a reference to the underlying block manager.
	BlockManager &GetBlockManager() const;
	//! Returns the optional client context.
	optional_ptr<ClientContext> GetClientContext() const;

private:
	//! The optional client context in which we use the partial block manager.
	//! The SingleFileCheckpointWriter contains a partial block manager, and the destructor of
	//! AttachedDatabase invokes CreateCheckpoint with that checkpoint writer.
	//! Thus, we're using the optional client context instead of a reference.
	optional_ptr<ClientContext> context;
	BlockManager &block_manager;
	PartialBlockType partial_block_type;
	mutex partial_block_lock;
	//! A map of (available space -> PartialBlock) for partially filled blocks
	//! This is a multimap because there might be outstanding partial blocks with
	//! the same amount of left-over space
	multimap<idx_t, unique_ptr<PartialBlock>> partially_filled_blocks;

	//! The maximum size (in bytes) at which a partial block will be considered a partial block
	uint32_t max_partial_block_size;
	uint32_t max_use_count;

private:
	//! Try to obtain a partially filled block that can fit "segment_size" bytes
	//! If successful, returns true and returns the block_id and offset_in_block to write to
	//! Otherwise, returns false
};

} // namespace duckdb
