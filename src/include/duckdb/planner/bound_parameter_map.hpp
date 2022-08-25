//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_parameter_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct BoundParameterData;

using bound_parameter_map_t = unordered_map<idx_t, shared_ptr<BoundParameterData>>;

} // namespace duckdb
