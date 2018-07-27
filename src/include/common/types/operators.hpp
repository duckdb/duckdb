
#pragma once

namespace operators {
//===--------------------------------------------------------------------===//
// Numeric Operations
//===--------------------------------------------------------------------===//
struct Addition {
	template <class T> static inline T Operation(T left, T right) {
		return left + right;
	}
};

struct Subtraction {
	template <class T> static inline T Operation(T left, T right) {
		return left - right;
	}
};

struct Multiplication {
	template <class T> static inline T Operation(T left, T right) {
		return left * right;
	}
};

struct Division {
	template <class T> static inline T Operation(T left, T right) {
		return left / right;
	}
};

//===--------------------------------------------------------------------===//
// Comparison Operations
//===--------------------------------------------------------------------===//
struct Equals {
	template <class T> static inline bool Operation(T left, T right) {
		return left == right;
	}
};

struct NotEquals {
	template <class T> static inline bool Operation(T left, T right) {
		return left != right;
	}
};

struct GreaterThan {
	template <class T> static inline bool Operation(T left, T right) {
		return left > right;
	}
};

struct GreaterThanEquals {
	template <class T> static inline bool Operation(T left, T right) {
		return left >= right;
	}
};

struct LessThan {
	template <class T> static inline bool Operation(T left, T right) {
		return left < right;
	}
};

struct LessThanEquals {
	template <class T> static inline bool Operation(T left, T right) {
		return left <= right;
	}
};

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

//===--------------------------------------------------------------------===//
// Aggregation Operations
//===--------------------------------------------------------------------===//
struct Max {
	template <class T> static inline T Operation(T left, T right) {
		return std::max(left, right);
	}
};

struct Min {
	template <class T> static inline T Operation(T left, T right) {
		return std::min(left, right);
	}
};

}
