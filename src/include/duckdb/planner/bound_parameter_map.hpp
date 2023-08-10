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
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

struct BoundParameterData;

using bound_parameter_map_t = case_insensitive_map_t<shared_ptr<BoundParameterData>>;

} // namespace duckdb
