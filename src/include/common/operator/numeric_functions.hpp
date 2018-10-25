//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/operators/numeric_functions.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

namespace operators {

struct Abs {
	template <class T> static inline T Operation(T left) {
		return abs(left);
	}
};

} // namespace operators
