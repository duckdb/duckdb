//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/deque.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/memory_safety.hpp"
#include <deque>

namespace duckdb {

template <class DATA_TYPE, bool SAFE = true>
class deque : public std::deque<DATA_TYPE, std::allocator<DATA_TYPE>> { // NOLINT: matching name of std
public:
	using original = std::deque<DATA_TYPE, std::allocator<DATA_TYPE>>;
	using original::original;
	using value_type = typename original::value_type;
	using allocator_type = typename original::allocator_type;
	using size_type = typename original::size_type;
	using difference_type = typename original::difference_type;
	using reference = typename original::reference;
	using const_reference = typename original::const_reference;
	using pointer = typename original::pointer;
	using const_pointer = typename original::const_pointer;
	using iterator = typename original::iterator;
	using const_iterator = typename original::const_iterator;
	using reverse_iterator = typename original::reverse_iterator;
	using const_reverse_iterator = typename original::const_reverse_iterator;

private:
	static inline void AssertIndexInBounds(idx_t index, idx_t size) {
#if defined(DUCKDB_DEBUG_NO_SAFETY) || defined(DUCKDB_CLANG_TIDY)
		return;
#else
		if (DUCKDB_UNLIKELY(index >= size)) {
			throw InternalException("Attempted to access index %ld within deque of size %ld", index, size);
		}
#endif
	}

public:
#ifdef DUCKDB_CLANG_TIDY
	[[clang::reinitializes]]
#endif
	inline void
	clear() noexcept { // NOLINT: hiding on purpose
		original::clear();
	}

	// Because we create the other constructor, the implicitly created constructor
	// gets deleted, so we have to be explicit
	deque() = default;
	deque(original &&other) : original(std::move(other)) { // NOLINT: allow implicit conversion
	}
	template <bool INTERNAL_SAFE>
	deque(deque<DATA_TYPE, INTERNAL_SAFE> &&other) : original(std::move(other)) { // NOLINT: allow implicit conversion
	}

	template <bool INTERNAL_SAFE = false>
	inline typename original::reference get(typename original::size_type __n) { // NOLINT: hiding on purpose
		if (MemorySafety<INTERNAL_SAFE>::ENABLED) {
			AssertIndexInBounds(__n, original::size());
		}
		return original::operator[](__n);
	}

	template <bool INTERNAL_SAFE = false>
	inline typename original::const_reference get(typename original::size_type __n) const { // NOLINT: hiding on purpose
		if (MemorySafety<INTERNAL_SAFE>::ENABLED) {
			AssertIndexInBounds(__n, original::size());
		}
		return original::operator[](__n);
	}

	typename original::reference operator[](typename original::size_type __n) { // NOLINT: hiding on purpose
		return get<SAFE>(__n);
	}
	typename original::const_reference operator[](typename original::size_type __n) const { // NOLINT: hiding on purpose
		return get<SAFE>(__n);
	}

	typename original::reference front() { // NOLINT: hiding on purpose
		if (MemorySafety<SAFE>::ENABLED && original::empty()) {
			throw InternalException("'front' called on an empty deque!");
		}
		return get<SAFE>(0);
	}

	typename original::const_reference front() const { // NOLINT: hiding on purpose
		if (MemorySafety<SAFE>::ENABLED && original::empty()) {
			throw InternalException("'front' called on an empty deque!");
		}
		return get<SAFE>(0);
	}

	typename original::reference back() { // NOLINT: hiding on purpose
		if (MemorySafety<SAFE>::ENABLED && original::empty()) {
			throw InternalException("'back' called on an empty deque!");
		}
		return get<SAFE>(original::size() - 1);
	}

	typename original::const_reference back() const { // NOLINT: hiding on purpose
		if (MemorySafety<SAFE>::ENABLED && original::empty()) {
			throw InternalException("'back' called on an empty deque!");
		}
		return get<SAFE>(original::size() - 1);
	}
};

template <typename T>
using unsafe_deque = deque<T, false>;

} // namespace duckdb
