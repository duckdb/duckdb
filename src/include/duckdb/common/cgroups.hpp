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

namespace duckdb {

class CGroups {
public:
	static optional_idx GetMemoryLimit();
	static idx_t GetCPULimit(idx_t physical_cores);
};

} // namespace duckdb
