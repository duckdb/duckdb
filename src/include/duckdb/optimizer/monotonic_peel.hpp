//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/monotonic_peel.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/common.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

struct MonotonicPeelStep {
	idx_t col_arg;
	bool inverts;
};

//! Inspect one wrapper level of `expr`. On success, sets `step.col_arg` to the column-bearing
//! child to walk into and `step.inverts` if this hop reverses ordering. Handles invertible
//! casts and any function whose ArgProperties on the unique non-foldable arg declare
//! monotonicity.
//!
//! `allow_finite_only`: tier-1 (false) refuses args where ArgProperties::requires_finite_input
//! is set; tier-2 (true) accepts them, leaving null-safety to the caller.
bool TryPeelMonotonicLevel(const Expression &expr, MonotonicPeelStep &step, bool allow_finite_only = false);

//! Reference to the column-bearing child after a successful peel.
const Expression &PeelColumnBearingChild(const Expression &expr, const MonotonicPeelStep &step);

//! Move the column-bearing child out of `expr` (mutates expr).
unique_ptr<Expression> ExtractColumnBearingChild(Expression &expr, const MonotonicPeelStep &step);

} // namespace duckdb
