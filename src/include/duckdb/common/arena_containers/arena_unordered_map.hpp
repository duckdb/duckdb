//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arena_containers/arena_unordered_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arena_stl_allocator.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

template <class KEY, class VALUE, class HASH = std::hash<KEY>, class PRED = std::equal_to<KEY>>
using arena_unordered_map = unordered_map<KEY, VALUE, HASH, PRED, arena_stl_allocator<pair<const KEY, VALUE>>>;

} // namespace duckdb
