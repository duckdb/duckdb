//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/unsafe_barrier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! unsafe(x) returns x, but marks the expression as having an effect that is observable from outside the query - it
//! may throw an error, or it may have side effects. The optimizer treats it as a barrier: an unsafe expression is
//! never moved to a position where it would be evaluated on rows that the operators around it filter out, and it is
//! always evaluated after the filters that sit next to it.
struct UnsafeBarrier {
	//! Whether this expression node is an unsafe() call
	static bool IsBarrier(const Expression &expr);
	//! Whether the expression tree contains an unsafe() call
	static bool Contains(const Expression &expr);
	//! Whether evaluating this expression is observable from the outside, and it must therefore be wrapped in an
	//! unsafe() barrier before it can be pushed into a secure view
	static bool Required(const Expression &expr);
	//! Wrap the given expression in an unsafe() barrier
	static unique_ptr<Expression> Wrap(unique_ptr<Expression> expr);
};

} // namespace duckdb
