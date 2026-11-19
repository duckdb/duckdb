//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arena_containers/arena_sql_value_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arena_stl_allocator.hpp"
#include "duckdb/common/types/sql_value_map.hpp"

namespace duckdb {

template <class KEY, class VALUE>
using arena_sql_value_map_t = unordered_map<KEY, VALUE, SQLValueHashFunction<KEY>, SQLValueEquality<KEY>,
                                            arena_stl_allocator<std::pair<const KEY, VALUE>>>;

template <class KEY, class VALUE>
using arena_sql_value_ordered_map_t =
    map<KEY, VALUE, SQLValueLessThan<KEY>, arena_stl_allocator<std::pair<const KEY, VALUE>>>;

} // namespace duckdb
