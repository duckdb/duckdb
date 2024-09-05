//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/stl_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

namespace duckdb {

void *stl_malloc(size_t size); // NOLINT: not using camelcase on purpose here
void stl_free(void *ptr);      // NOLINT: not using camelcase on purpose here

template <class T>
T *stl_new_array_uninitialized(size_t size) {
	return static_cast<T *>(stl_malloc(size * sizeof(T)));
}

template <class T>
T *stl_new_array(size_t size) {
	auto result = stl_new_array_uninitialized<T>(size);
	return new (result) T[size]();
}

template <typename T>
struct stl_default_delete { // NOLINT: not using camelcase on purpose here
	static_assert(!std::is_function<T>::value, "default_delete cannot be instantiated for function types");

	stl_default_delete() noexcept = default;

	template <class U>
	stl_default_delete(const stl_default_delete<U> &, // NOLINT: allow implicit conversion
	                   typename std::enable_if<std::is_convertible<U *, T *>::value>::type * = nullptr) noexcept {
	}

	void operator()(T *ptr) const noexcept {
		static_assert(sizeof(T) != 0, "cannot delete an incomplete type");
		static_assert(!std::is_void<T>::value, "cannot delete an incomplete type");
		stl_free(ptr);
	}
};

template <class T>
struct stl_default_delete<T[]> { // NOLINT: not using camelcase on purpose here
private:
	template <class U>
	struct _EnableIfConvertible // NOLINT: hiding on purpose
	    : std::enable_if<std::is_convertible<U (*)[], T (*)[]>::value> {};

public:
	stl_default_delete() noexcept = default;

	template <class U>
	stl_default_delete(const stl_default_delete<U[]> &, // NOLINT: not using camelcase on purpose here
	                   typename _EnableIfConvertible<U>::type * = nullptr) noexcept {
	}

	template <class U>
	typename _EnableIfConvertible<U>::type operator()(U *ptr) const noexcept { // NOLINT: matching std
		static_assert(sizeof(U) != 0, "cannot delete an incomplete type");
		delete[] ptr;
	}
};

template <class T>
class stl_allocator { // NOLINT: not using camelcase on purpose here
public:
	using original = std::allocator<T>;
	using value_type = typename original::value_type;
	using pointer = typename original::pointer;
	using const_pointer = typename original::const_pointer;
	using reference = typename original::reference;
	using const_reference = typename original::const_reference;
	using size_type = typename original::size_type;
	using difference_type = typename original::difference_type;
	using propagate_on_container_move_assignment = typename original::propagate_on_container_move_assignment;
	template <class U>
	struct rebind {
		typedef stl_allocator<U> other;
	};
	using is_always_equal = typename original::is_always_equal;

public:
	stl_allocator() noexcept {
	}
	stl_allocator(const stl_allocator &) noexcept {
	}
	template <typename U>
	stl_allocator(const stl_allocator<U> &) noexcept { // NOLINT: allow implicit conversion
	}
	~stl_allocator() {
	}

public:
	pointer allocate(size_type count, const void * = 0) { // NOLINT: matching name of std
		return static_cast<pointer>(stl_malloc(count * sizeof(value_type)));
	}

	void deallocate(T *p, size_type) { // NOLINT: matching name of std
		stl_free(p);
	}
};

template <class T, class U>
bool operator==(const stl_allocator<T> &, const stl_allocator<U> &) noexcept {
	return true;
}

template <class T, class U>
bool operator!=(const stl_allocator<T> &, const stl_allocator<U> &) noexcept {
	return false;
}

} // namespace duckdb
