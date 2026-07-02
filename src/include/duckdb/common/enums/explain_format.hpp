//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/explain_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

enum class ExplainFormat : uint8_t { DEFAULT, TEXT, JSON, HTML, GRAPHVIZ, YAML, MERMAID };

} // namespace duckdb
