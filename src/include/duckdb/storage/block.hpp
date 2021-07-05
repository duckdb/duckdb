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
class DatabaseInstance;

class Block : public FileBuffer {
public:
	Block(DatabaseInstance &db, block_id_t id);

	block_id_t id;
};

struct BlockPointer {
	block_id_t block_id;
	uint32_t offset;
};

} // namespace duckdb
