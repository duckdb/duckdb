//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/left_to_prefix.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// This rule rewrites equality comparisons on string prefixes, e.g. [left(s, 3) = 'abc'] into
// [prefix(s, 'abc')], which can be pushed into the scan as a range filter.
class LeftToPrefixRule : public Rule {
public:
	explicit LeftToPrefixRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
