//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/stl_allocator.hpp"

#include <list>

namespace duckdb {

template <class T, class ALLOCATOR = stl_allocator<T>>
using list = std::list<T, ALLOCATOR>;

} // namespace duckdb
