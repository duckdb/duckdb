//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/optional_unique_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

template <typename T>
class optional_unique_ptr {
public:
	using innerType = T;
	optional_unique_ptr() : inner(nullptr) {
	}
	optional_unique_ptr(T *ptr) : inner(ptr) {
	}
	optional_unique_ptr(unique_ptr<T> &&ptr_p) : inner(std::move(ptr_p)) {
	}
	optional_unique_ptr(optional_unique_ptr<T> &&ptr_p) : inner(std::move(ptr_p.inner)) {
	}
	explicit operator unique_ptr<T> &&() {
		return std::move(inner);
	}
	operator unique_ptr<T> &() {
		return inner;
	}
	operator const unique_ptr<T> &() const {
		return inner;
	}
	optional_unique_ptr &operator=(optional_unique_ptr &&r) {
		inner = std::move(r.inner);
		return *this;
	}
	T *get() {
		return inner.get();
	}
	const T *get() const {
		return inner.get();
	}
	operator bool() const {
		return !!inner;
	}
	T &operator*() {
		return *inner;
	}
	const T &operator*() const {
		return *inner;
	}
	T *operator->() {
		return inner.get();
	}
	const T *operator->() const {
		return inner.get();
	}
	unique_ptr<T> inner;
};

} // namespace duckdb
