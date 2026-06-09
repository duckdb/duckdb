//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/explain_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

enum class ExplainFormat : uint8_t { DEFAULT, TEXT, JSON, HTML, GRAPHVIZ, YAML, MERMAID };

//! Resolve an explain format name (case-insensitive) to an ExplainFormat. Throws InvalidInputException listing the
//! valid format names when the name is not recognized. This is the single source of truth for the set of explain
//! format names, shared by the EXPLAIN parser, the query profiler, and the TreeRenderer factory.
ExplainFormat ExplainFormatFromString(const string &name);
//! Returns the canonical (lowercase) name for an ExplainFormat.
string ExplainFormatToString(ExplainFormat format);

} // namespace duckdb
