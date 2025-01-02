//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/common/stl_allocator.hpp"

#include <map>

namespace duckdb {

template <class KEY, class VALUE, class COMPARE = std::less<KEY>,
          class ALLOCATOR = stl_allocator<pair<const KEY, VALUE>>>
using map = std::map<KEY, VALUE, COMPARE, ALLOCATOR>;

template <class KEY, class VALUE, class COMPARE = std::less<KEY>,
          class ALLOCATOR = stl_allocator<pair<const KEY, VALUE>>>
using multimap = std::multimap<KEY, VALUE, COMPARE, ALLOCATOR>;

} // namespace duckdb
