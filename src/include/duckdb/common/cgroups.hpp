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
	static optional_idx GetMemoryLimit(FileSystem &fs);
	static idx_t GetCPULimit(FileSystem &fs, idx_t physical_cores);

private:
	static optional_idx GetCGroupV2MemoryLimit(FileSystem &fs);
	static optional_idx GetCGroupV1MemoryLimit(FileSystem &fs);
	static string ReadCGroupPath(FileSystem &fs, const char *cgroup_file);
	static string ReadMemoryCGroupPath(FileSystem &fs, const char *cgroup_file);
	static optional_idx ReadCGroupValue(FileSystem &fs, const char *file_path);
};

} // namespace duckdb
