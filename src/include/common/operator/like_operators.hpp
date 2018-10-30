//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/operator/like_operators.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>

namespace operators {

struct Like {
	static bool Operation(const char *left, const char *right,
	                      const char *escape = nullptr);
};

struct NotLike {
	static inline bool Operation(const char *left, const char *right,
	                             const char *escape = nullptr) {
		return !Like::Operation(left, right, escape);
	}
};

} // namespace operators
