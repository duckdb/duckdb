//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/operator/aggregate_operators.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>

namespace operators {

struct Min {
	template <class T> static inline T Operation(T left, T right) {
		return std::min(left, right);
	}
};

struct Max {
	template <class T> static inline T Operation(T left, T right) {
		return std::max(left, right);
	}
};

struct MaximumStringLength {
	static inline uint64_t Operation(const char *str, uint64_t right) {
		return std::max((uint64_t)strlen(str), right);
	}
};

struct AnyTrue {
	static inline uint8_t Operation(uint8_t val, uint8_t result) {
		return result || val == true;
	}
};

struct AllTrue {
	static inline uint8_t Operation(uint8_t val, uint8_t result) {
		return result && val == true;
	}
};

} // namespace operators
