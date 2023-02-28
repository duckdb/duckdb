//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/temporary_file_information.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct TemporaryFileInformation {
	string path;
	idx_t size;
};

} // namespace duckdb
