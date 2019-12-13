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
	Block(block_id_t id);

	block_id_t id;
};

} // namespace duckdb
