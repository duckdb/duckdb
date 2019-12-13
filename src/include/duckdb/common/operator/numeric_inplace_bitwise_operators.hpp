//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/numeric_inplace_bitwise_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

struct BitwiseXORInPlace {
	template <class T> static inline void Operation(T &left, T right) {
		left ^= right;
	}
};

} // namespace duckdb
