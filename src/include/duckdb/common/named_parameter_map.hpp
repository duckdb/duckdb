//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/named_parameter_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types.hpp"
namespace duckdb {

using named_parameter_type_map_t = case_insensitive_map_t<LogicalType>;
using named_parameter_map_t = case_insensitive_map_t<Value>;

} // namespace duckdb
