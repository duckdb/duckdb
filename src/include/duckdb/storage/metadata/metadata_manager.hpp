//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/metadata/metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {
class DatabaseInstance;

struct MetadataBlock {
	shared_ptr<BlockHandle> block;
	block_id_t block_id;
	vector<uint8_t> free_blocks;

	void Write(WriteStream &sink);
	static MetadataBlock Read(ReadStream &source);

	idx_t FreeBlocksToInteger();
	void FreeBlocksFromInteger(idx_t blocks);
};

struct MetadataPointer {
	idx_t block_index : 56;
	uint8_t index : 8;
};

struct MetadataHandle {
	MetadataPointer pointer;
	BufferHandle handle;
};

class MetadataManager {
public:
	//! The size of metadata blocks
	static constexpr const idx_t METADATA_BLOCK_SIZE = 4088;
	//! The amount of metadata blocks per storage block
	static constexpr const idx_t METADATA_BLOCK_COUNT = 64;

public:
	MetadataManager(BlockManager &block_manager, BufferManager &buffer_manager);
	~MetadataManager();

	MetadataHandle AllocateHandle();
	MetadataHandle Pin(MetadataPointer pointer);

	MetaBlockPointer GetDiskPointer(MetadataPointer pointer, uint32_t offset = 0);
	MetadataPointer FromDiskPointer(MetaBlockPointer pointer);
	MetadataPointer RegisterDiskPointer(MetaBlockPointer pointer);

	static BlockPointer ToBlockPointer(MetaBlockPointer meta_pointer);
	static MetaBlockPointer FromBlockPointer(BlockPointer block_pointer);

	//! Flush all blocks to disk
	void Flush();

	void MarkBlocksAsModified();
	void ClearModifiedBlocks(const vector<MetaBlockPointer> &pointers);

	idx_t BlockCount();

	void Write(WriteStream &sink);
	void Read(ReadStream &source);

protected:
	BlockManager &block_manager;
	BufferManager &buffer_manager;
	unordered_map<block_id_t, MetadataBlock> blocks;
	unordered_map<block_id_t, idx_t> modified_blocks;

protected:
	block_id_t AllocateNewBlock();
	block_id_t GetNextBlockId();

	void AddBlock(MetadataBlock new_block, bool if_exists = false);
	void AddAndRegisterBlock(MetadataBlock block);
	void ConvertToTransient(MetadataBlock &block);
};

} // namespace duckdb
