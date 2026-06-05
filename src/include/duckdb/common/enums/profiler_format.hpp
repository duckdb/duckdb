//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/profiler_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class ProfilerPrintFormat : uint8_t { QUERY_TREE, JSON, QUERY_TREE_OPTIMIZER, NO_OUTPUT, HTML, GRAPHVIZ, MERMAID };

} // namespace duckdb
