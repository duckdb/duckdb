//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/operators/numeric_inplace_bitwise_operators.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

namespace operators {

struct BitwiseXORInPlace {
	template <class T> static inline void Operation(T &left, T right) {
		left ^= right;
	}
};

} // namespace operators
