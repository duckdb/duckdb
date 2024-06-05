//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/join_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Join Types
//===--------------------------------------------------------------------===//
enum class JoinType : uint8_t {
	INVALID = 0, // invalid join type
	LEFT = 1,    // left
	RIGHT = 2,   // right
	INNER = 3,   // inner
	OUTER = 4,   // outer
	SEMI = 5,    // LEFT SEMI join returns left side row ONLY if it has a join partner, no duplicates.
	ANTI = 6,    // LEFT ANTI join returns left side row ONLY if it has NO join partner, no duplicates
	MARK = 7,    // MARK join returns marker indicating whether or not there is a join partner (true), there is no join
	             // partner (false)
	SINGLE = 8,  // SINGLE join is like LEFT OUTER JOIN, BUT returns at most one join partner per entry on the LEFT side
	             // (and NULL if no partner is found)
	RIGHT_SEMI = 9, // RIGHT SEMI join is created by the optimizer when the children of a semi join need to be switched
	                // so that the build side can be the smaller table
	RIGHT_ANTI = 10 // RIGHT ANTI join is created by the optimizer when the children of an anti join need to be
	                // switched so that the build side can be the smaller table
};

//! True if join is left or full outer join
bool IsLeftOuterJoin(JoinType type);

//! True if join is rght or full outer join
bool IsRightOuterJoin(JoinType type);

//! Whether the build side is propagated out of the join
bool PropagatesBuildSide(JoinType type);

//! Whether the JoinType has an inverse
bool HasInverseJoinType(JoinType type);

//! Gets the inverse JoinType, e.g., LEFT -> RIGHT
JoinType InverseJoinType(JoinType type);

// **DEPRECATED**: Use EnumUtil directly instead.
string JoinTypeToString(JoinType type);

} // namespace duckdb
