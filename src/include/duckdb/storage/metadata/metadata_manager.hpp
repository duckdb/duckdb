//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/metadata/metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/serializer.hpp"
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
	static constexpr const idx_t METADATA_BLOCK_SIZE = 4090;
	//! The amount of metadata blocks per storage block
	static constexpr const idx_t METADATA_BLOCK_COUNT = 64;
public:
	MetadataManager(BlockManager &block_manager, BufferManager &buffer_manager);
	~MetadataManager();

	MetadataHandle AllocateHandle();
	MetadataHandle Pin(MetadataPointer pointer);

	MetaBlockPointer GetDiskPointer(MetadataPointer pointer, uint32_t offset = 0);
	MetadataPointer FromDiskPointer(MetaBlockPointer pointer);

	//! Flush all blocks to disk
	void Flush();

	void MarkWrittenBlocks();

protected:
	BlockManager &block_manager;
	BufferManager &buffer_manager;
	vector<MetadataBlock> blocks;
	vector<idx_t> free_blocks;

protected:
	void AllocateNewBlock();
	block_id_t GetNextBlockId();

};

} // namespace duckdb
