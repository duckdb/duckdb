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

struct BenchmarkConfiguration {
	std::string name_pattern {};
	BenchmarkMetaType meta = BenchmarkMetaType::NONE;
	bool print_profile_info = false;
};

} // namespace duckdb
