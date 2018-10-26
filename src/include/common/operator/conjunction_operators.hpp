//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/operator/conjunction_operators.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

namespace operators {

struct And {
	static inline bool Operation(bool left, bool right) {
		return left && right;
	}
};

struct Or {
	static inline bool Operation(bool left, bool right) {
		return left || right;
	}
};

} // namespace operators
