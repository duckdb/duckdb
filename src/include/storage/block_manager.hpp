//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/file_system.hpp"
#include "common/fstream_util.hpp"
#include "storage/block.hpp"
#include "storage/data_table.hpp"
#include "storage/meta_block_reader.hpp"

namespace duckdb {
struct MetaBlockWriter;
//! BlockManager is an abstract representation to manage blocks on duckDB. When writing or reading blocks, the
//! blockManager creates and accesses blocks. The concrete types implements how blocks are stored.
class BlockManager {
public:
	// It is mandatory to have a default destructor for a base class, so the proper derived destructor is called
	virtual ~BlockManager() = default;
	//! Fetches an existing block by its ID
	virtual unique_ptr<Block> GetBlock(block_id_t id) = 0;
	//! Creates a new block inside the block manager
	virtual unique_ptr<Block> CreateBlock() = 0;
	//! Flushes a block to disk
	virtual void Flush(unique_ptr<Block> &block) = 0;
	//! Appends the data chunk to a block
	virtual void AppendDataToBlock(DataChunk &chunk, unique_ptr<Block> &block, MetaBlockWriter &meta_writer) = 0;
	//! Loads the table info and data to memory
	virtual void LoadTableData(DataTable &table, DataChunk &chunk, unique_ptr<Block> meta_block) = 0;
	//! Returns the id of the first data block
	virtual block_id_t GetMetadataBlockId() = 0;
	//! Sets the id of the first data block
	virtual void SetMetadataBlockId(block_id_t block_id) = 0;
};
} // namespace duckdb