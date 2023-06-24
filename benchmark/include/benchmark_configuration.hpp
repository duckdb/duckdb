//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// benchmark_configuration.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

enum class BenchmarkMetaType { NONE, INFO, QUERY };
enum class BenchmarkProfileInfo { NONE, NORMAL, DETAILED };

struct BenchmarkConfiguration {
	string name_pattern {};
	BenchmarkMetaType meta = BenchmarkMetaType::NONE;
	BenchmarkProfileInfo profile_info = BenchmarkProfileInfo::NONE;
};

} // namespace duckdb
