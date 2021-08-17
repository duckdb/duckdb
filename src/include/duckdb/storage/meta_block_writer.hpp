//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/meta_block_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/common/set.hpp"

namespace duckdb {
class DatabaseInstance;

//! This struct is responsible for writing metadata to disk
class MetaBlockWriter : public Serializer {
public:
	MetaBlockWriter(DatabaseInstance &db, block_id_t initial_block_id = INVALID_BLOCK);
	~MetaBlockWriter() override;

	DatabaseInstance &db;
	unique_ptr<Block> block;
	set<block_id_t> written_blocks;
	idx_t offset;

public:
	BlockPointer GetBlockPointer();
	void Flush();

	void WriteData(const_data_ptr_t buffer, idx_t write_size) override;

protected:
	virtual block_id_t GetNextBlockId();
};

} // namespace duckdb
