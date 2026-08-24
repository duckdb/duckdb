//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/monotone_preimage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

//! Rewrites `f(col) OP const` into a direct range predicate on `col` when `f` is a monotone
//! (ArgProperties) unary function over an exact integer/date column. The preimage boundary is
//! found by bisecting the column's value domain, generalizing the hand-written inverses in
//! move_constants / date_trunc_simplification / timestamp_comparison. Exact-or-bail: if an exact
//! boundary cannot be established the rule leaves the expression untouched.
class MonotonePreimageRule : public Rule {
public:
	explicit MonotonePreimageRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
