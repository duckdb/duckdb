//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/optional_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"

namespace duckdb {

template <class T>
class optional_ptr {
public:
	optional_ptr() : ptr(nullptr) {
	}
	optional_ptr(T *ptr_p) : ptr(ptr_p) {
	} // NOLINT: allow implicit creation from pointer

	operator bool() const {
		return ptr;
	}
	T &operator*() {
		if (!ptr) {
			throw InternalException("Attempting to dereference an optional pointer that is not set");
		}
		return *ptr;
	}
	T *operator->() {
		if (!ptr) {
			throw InternalException("Attempting to reference an optional pointer that is not set");
		}
		return ptr;
	}

private:
	T *ptr;
};

} // namespace duckdb
