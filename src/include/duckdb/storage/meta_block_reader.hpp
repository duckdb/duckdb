//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/meta_block_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {
//! This struct is responsible for reading meta data from disk
class MetaBlockReader : public Deserializer {
public:
	MetaBlockReader(BufferManager &manager, block_id_t block);

	BufferManager &manager;
	unique_ptr<BufferHandle> handle;
	idx_t offset;
	block_id_t next_block;

public:
	//! Read content of size read_size into the buffer
	void ReadData(data_ptr_t buffer, idx_t read_size) override;

private:
	void ReadNewBlock(block_id_t id);
};
} // namespace duckdb
