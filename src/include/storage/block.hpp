//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/block.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/storage_info.hpp"
#include "common/file_buffer.hpp"

namespace duckdb {

class Block : public FileBuffer {
public:
	Block(block_id_t id);

	block_id_t id;
};

} // namespace duckdb
