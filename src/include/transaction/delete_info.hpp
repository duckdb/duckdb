//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/delete_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/constants.hpp"

namespace duckdb {
class ChunkInfo;
class DataTable;

struct DeleteInfo {
	ChunkInfo *vinfo;
	index_t count;
	row_t rows[1];

	DataTable &GetTable();
};

}
