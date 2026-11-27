//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/in_clause.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Minimum IN child count (including the probe) for rewriting a scalar list to a hash join.
//! Scan-filter evaluation uses the same cutoff for small literal lists.
static constexpr idx_t IN_CLAUSE_REWRITE_THRESHOLD = 6;

} // namespace duckdb
