//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/stl_allocator.hpp"

#include <set>

namespace duckdb {

template <class ELEMENT, class COMPARE = std::less<ELEMENT>, class ALLOCATOR = stl_allocator<ELEMENT>>
using set = std::set<ELEMENT, COMPARE, ALLOCATOR>;

template <class ELEMENT>
using static_set = std::set<ELEMENT>;

template <class ELEMENT, class COMPARE = std::less<ELEMENT>, class ALLOCATOR = stl_allocator<ELEMENT>>
using multiset = std::multiset<ELEMENT, COMPARE, ALLOCATOR>;

} // namespace duckdb
