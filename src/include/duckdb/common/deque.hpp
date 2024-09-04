//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/deque.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/stl_allocator.hpp"

#include <deque>

namespace duckdb {

template <class T, class ALLOCATOR = stl_allocator<T>>
using deque = std::deque<T, ALLOCATOR>;

}
