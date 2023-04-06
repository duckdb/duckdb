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
#include <vector>

namespace duckdb {

// TODO: inline this, needs changes to 'exception.hpp' and other headers to avoid circular dependency
void AssertIndexInBounds(idx_t index, idx_t size);

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

	typename original::reference operator[](typename original::size_type __n) {
#ifdef DEBUG
		AssertIndexInBounds(__n, original::size());
#endif
		return original::operator[](__n);
	}
	typename original::const_reference operator[](typename original::size_type __n) const {
#ifdef DEBUG
		AssertIndexInBounds(__n, original::size());
#endif
		return original::operator[](__n);
	}
};

} // namespace duckdb
