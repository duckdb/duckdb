//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/cgroups.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class CGroups {
public:
	static optional_idx GetMemoryLimit(BaseFileSystem &fs);
	static idx_t GetCPULimit(BaseFileSystem &fs, idx_t physical_cores);
};

} // namespace duckdb
