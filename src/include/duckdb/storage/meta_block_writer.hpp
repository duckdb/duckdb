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

namespace duckdb {

//! This struct is responsible for writing metadata to disk
class MetaBlockWriter : public Serializer {
public:
	MetaBlockWriter(BlockManager &manager);
	~MetaBlockWriter();

	BlockManager &manager;
	unique_ptr<Block> block;
	index_t offset;

public:
	void Flush();

	void WriteData(const_data_ptr_t buffer, index_t write_size) override;
};

} // namespace duckdb
