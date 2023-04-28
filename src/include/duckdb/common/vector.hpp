//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/exception.hpp"
#include <vector>

namespace duckdb {

namespace {
struct __vector_utils {
	static inline void AssertIndexInBounds(idx_t index, idx_t size) {
		//#ifdef DEBUG
		if (DUCKDB_UNLIKELY(index >= size)) {
			throw InternalException("Attempted to access index %ld within vector of size %ld", index, size);
		}
		//#endif
	}
};
} // namespace

template <class _Tp, class _Allocator = std::allocator<_Tp>>
class vector : public std::vector<_Tp, _Allocator> {
public:
	using original = std::vector<_Tp, _Allocator>;
	using original::original;
	using size_type = typename original::size_type;
	using const_reference = typename original::const_reference;
	using reference = typename original::reference;

#ifdef DUCKDB_CLANG_TIDY
	// This is necessary to tell clang-tidy that it reinitializes the variable after a move
	[[clang::reinitializes]]
#endif
	inline void
	clear() noexcept {
		original::clear();
	}

	// Because we create the other constructor, the implicitly created constructor
	// gets deleted, so we have to be explicit
	vector() = default;
	vector(original &&other) : original(std::move(other)) {
	}

	template <bool UNSAFE = true>
	inline typename original::reference get(typename original::size_type __n) {
		if (!UNSAFE) {
			__vector_utils::AssertIndexInBounds(__n, original::size());
		}
		return original::operator[](__n);
	}

	template <bool UNSAFE = true>
	inline typename original::const_reference get(typename original::size_type __n) const {
		if (!UNSAFE) {
			__vector_utils::AssertIndexInBounds(__n, original::size());
		}
		return original::operator[](__n);
	}

	typename original::reference operator[](typename original::size_type __n) {
		return get<false>(__n);
	}
	typename original::const_reference operator[](typename original::size_type __n) const {
		return get<false>(__n);
	}
};

} // namespace duckdb
