//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/file_buffer.hpp"

namespace duckdb {

class Block : public FileBuffer {
public:
	Block(Allocator &allocator, block_id_t id);
	Block(FileBuffer &source, block_id_t id);

	block_id_t id;
};

struct BlockPointer {
	block_id_t block_id;
	uint32_t offset;
};

} // namespace duckdb
