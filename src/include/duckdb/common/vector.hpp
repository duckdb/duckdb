//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include <vector>

using std::allocator;

namespace duckdb {

void AssertIndexInBounds(idx_t index, idx_t size);

template <class _Tp, typename __Allocator = allocator<_Tp>>
class vector : public std::vector<_Tp, __Allocator> {
public:
	using original = std::vector<_Tp, __Allocator>;
	using original::original;
	using size_type = typename original::size_type;
	using const_reference = typename original::const_reference;
	using reference = typename original::reference;

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
