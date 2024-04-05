//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/shared_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <type_traits>

template <typename T>
class weak_ptr;

namespace duckdb {

template <typename T>
class shared_ptr {
private:
	template <class U>
	friend class weak_ptr;
	std::shared_ptr<T> internal;

public:
	// Constructors
	shared_ptr() : internal() {
	}
	shared_ptr(std::nullptr_t) : internal(nullptr) {
	} // Implicit conversion
	template <typename U>
	explicit shared_ptr(U *ptr) : internal(ptr) {
	}
	shared_ptr(const shared_ptr &other) : internal(other.internal) {
	}
	shared_ptr(std::shared_ptr<T> other) : internal(std::move(other)) {
	}

	// Destructor
	~shared_ptr() = default;

	// Assignment operators
	shared_ptr &operator=(const shared_ptr &other) {
		internal = other.internal;
		return *this;
	}

	// Modifiers
	void reset() {
		internal.reset();
	}

	template <typename U>
	void reset(U *ptr) {
		internal.reset(ptr);
	}

	template <typename U, typename Deleter>
	void reset(U *ptr, Deleter deleter) {
		internal.reset(ptr, deleter);
	}

	// Observers
	T *get() const {
		return internal.get();
	}

	long use_count() const {
		return internal.use_count();
	}

	explicit operator bool() const noexcept {
		return internal.operator bool();
	}

	// Element access
	std::__add_lvalue_reference_t<T> operator*() const {
		return *internal;
	}

	T *operator->() const {
		return internal.operator->();
	}

	// Relational operators
	template <typename U>
	bool operator==(const shared_ptr<U> &other) const noexcept {
		return internal == other.internal;
	}

	bool operator==(std::nullptr_t) const noexcept {
		return internal == nullptr;
	}

	template <typename U>
	bool operator!=(const shared_ptr<U> &other) const noexcept {
		return internal != other.internal;
	}

	template <typename U>
	bool operator<(const shared_ptr<U> &other) const noexcept {
		return internal < other.internal;
	}

	template <typename U>
	bool operator<=(const shared_ptr<U> &other) const noexcept {
		return internal <= other.internal;
	}

	template <typename U>
	bool operator>(const shared_ptr<U> &other) const noexcept {
		return internal > other.internal;
	}

	template <typename U>
	bool operator>=(const shared_ptr<U> &other) const noexcept {
		return internal >= other.internal;
	}
};

} // namespace duckdb
