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

template <class T1, class T2>
bool operator==(const stl_allocator<T1> &, const stl_allocator<T2> &) noexcept {
	return true;
}

template <class T1, class T2>
bool operator!=(const stl_allocator<T1> &, const stl_allocator<T2> &) noexcept {
	return false;
}

} // namespace duckdb
