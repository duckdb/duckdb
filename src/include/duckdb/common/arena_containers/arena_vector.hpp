//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arena_containers/arena_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arena_stl_allocator.hpp"

namespace duckdb {

template <class T>
using arena_vector = vector<T, true, arena_stl_allocator<T>>;

template <class T>
using unsafe_arena_vector = vector<T, false, arena_stl_allocator<T>>;

} // namespace duckdb
