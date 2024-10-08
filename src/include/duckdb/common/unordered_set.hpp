//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/unordered_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/stl_allocator.hpp"

#include <unordered_set>

namespace duckdb {

template <class ELEMENT, class HASH = std::hash<ELEMENT>, class EQUAL = std::equal_to<ELEMENT>,
          class ALLOCATOR = stl_allocator<ELEMENT>>
using unordered_set = std::unordered_set<ELEMENT, HASH, EQUAL, ALLOCATOR>;

template <class ELEMENT, class HASH = std::hash<ELEMENT>, class EQUAL = std::equal_to<ELEMENT>>
using static_unordered_set = std::unordered_set<ELEMENT, HASH, EQUAL>;

} // namespace duckdb
