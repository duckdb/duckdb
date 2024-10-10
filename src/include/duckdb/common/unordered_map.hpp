//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/common/stl_allocator.hpp"

#include <unordered_map>

namespace duckdb {

template <class KEY, class VALUE, class HASH = std::hash<KEY>, class EQUAL = std::equal_to<KEY>,
          class ALLOCATOR = stl_allocator<pair<const KEY, VALUE>>>
using unordered_map = std::unordered_map<KEY, VALUE, HASH, EQUAL, ALLOCATOR>;

template <class KEY, class VALUE, class HASH = std::hash<KEY>, class EQUAL = std::equal_to<KEY>>
using static_unordered_map = std::unordered_map<KEY, VALUE, HASH, EQUAL>;

} // namespace duckdb
