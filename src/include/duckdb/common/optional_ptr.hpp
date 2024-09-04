//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/optional_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

template <class T, bool SAFE = true>
class optional_ptr { // NOLINT: mimic std casing
public:
	optional_ptr() noexcept : ptr(nullptr) {
	}
	optional_ptr(T *ptr_p) : ptr(ptr_p) { // NOLINT: allow implicit creation from pointer
	}
	optional_ptr(T &ref) : ptr(&ref) { // NOLINT: allow implicit creation from reference
	}
	optional_ptr(const unique_ptr<T> &ptr_p) : ptr(ptr_p.get()) { // NOLINT: allow implicit creation from unique pointer
	}
	optional_ptr(const shared_ptr<T> &ptr_p) : ptr(ptr_p.get()) { // NOLINT: allow implicit creation from shared pointer
	}

	void CheckValid() const {
		if (MemorySafety<SAFE>::ENABLED) {
			if (!ptr) {
				throw InternalException("Attempting to dereference an optional pointer that is not set");
			}
		}
	}

	operator bool() const { // NOLINT: allow implicit conversion to bool
		return ptr;
	}
	T &operator*() {
		CheckValid();
		return *ptr;
	}
	const T &operator*() const {
		CheckValid();
		return *ptr;
	}
	T *operator->() {
		CheckValid();
		return ptr;
	}
	const T *operator->() const {
		CheckValid();
		return ptr;
	}
	T *get() { // NOLINT: mimic std casing
		// CheckValid();
		return ptr;
	}
	const T *get() const { // NOLINT: mimic std casing
		// CheckValid();
		return ptr;
	}
	// this looks dirty - but this is the default behavior of raw pointers
	T *get_mutable() const { // NOLINT: mimic std casing
		// CheckValid();
		return ptr;
	}

	bool operator==(const optional_ptr<T> &rhs) const {
		return ptr == rhs.ptr;
	}

	bool operator!=(const optional_ptr<T> &rhs) const {
		return ptr != rhs.ptr;
	}

private:
	T *ptr;
};

template <typename T>
using unsafe_optional_ptr = optional_ptr<T, false>;

} // namespace duckdb
