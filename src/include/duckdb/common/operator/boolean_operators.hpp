//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/operator/boolean_operators.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

struct Not {
	static inline bool Operation(bool left) {
		return !left;
	}
};

/*
SQL AND Rules:

TRUE  AND TRUE   = TRUE
TRUE  AND FALSE  = FALSE
TRUE  AND NULL   = NULL
FALSE AND TRUE   = FALSE
FALSE AND FALSE  = FALSE
FALSE AND NULL   = FALSE
NULL  AND TRUE   = NULL
NULL  AND FALSE  = FALSE
NULL  AND NULL   = NULL

Basically:
- Only true if both are true
- False if either is false (regardless of NULLs)
- NULL otherwise
*/
struct And {
	static inline bool Operation(bool left, bool right) {
		return left && right;
	}
};

struct AndMask {
	static inline bool Operation(bool left, bool right, bool left_null, bool right_null) {
		return (left_null && (right_null || right)) || (right_null && left);
	}
};

/*
SQL OR Rules:

OR
TRUE  OR TRUE  = TRUE
TRUE  OR FALSE = TRUE
TRUE  OR NULL  = TRUE
FALSE OR TRUE  = TRUE
FALSE OR FALSE = FALSE
FALSE OR NULL  = NULL
NULL  OR TRUE  = TRUE
NULL  OR FALSE = NULL
NULL  OR NULL  = NULL

Basically:
- Only false if both are false
- True if either is true (regardless of NULLs)
- NULL otherwise
*/
struct Or {
	static inline bool Operation(bool left, bool right) {
		return left || right;
	}
};

struct OrMask {
	static inline bool Operation(bool left, bool right, bool left_null, bool right_null) {
		return (left_null && (right_null || !right)) || (right_null && !left);
	}
};
} // namespace duckdb
