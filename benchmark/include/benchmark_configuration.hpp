//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// benchmark_configuration.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

namespace duckdb {

enum class BenchmarkMetaType { NONE, INFO, QUERY };
enum class BenchmarkProfileInfo { NONE, NORMAL, DETAILED };

struct BenchmarkConfiguration {
	std::string name_pattern {};
	BenchmarkMetaType meta = BenchmarkMetaType::NONE;
	BenchmarkProfileInfo profile_info = BenchmarkProfileInfo::NONE;
};

} // namespace duckdb
