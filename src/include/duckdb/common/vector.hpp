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

template <class _Tp, class _Allocator = allocator<_Tp>>
class vector : public std::vector<_Tp, _Allocator> {
public:
	using original = std::vector<_Tp, _Allocator>;
	using original::original;

	typename original::reference operator[](typename original::size_type __n) {
		AssertIndexInBounds(__n, original::size());
		return original::operator[](__n);
	}
	typename original::const_reference operator[](typename original::size_type __n) const {
		AssertIndexInBounds(__n, original::size());
		return original::operator[](__n);
	}
};

} // namespace duckdb
