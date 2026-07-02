//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arena_containers/arena_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

//! Call destructor without attempting to free the memory
template <class T>
struct arena_deleter { // NOLINT: match stl case
	void operator()(T *pointer) {
		pointer->~T();
	}
};

template <class T>
using arena_ptr = unique_ptr<T, arena_deleter<T>>;

template <class T>
using unsafe_arena_ptr = unique_ptr<T, arena_deleter<T>, false>;

} // namespace duckdb
