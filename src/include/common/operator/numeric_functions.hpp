//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/operator/numeric_functions.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>

namespace operators {

struct Abs {
	template <class T> static inline T Operation(T left) {
		return left < 0 ? left * -1 : left;
	}
};

} // namespace operators
