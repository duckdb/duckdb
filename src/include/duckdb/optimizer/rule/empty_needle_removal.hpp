//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/empty_needle_removal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// Simplifies string functions with a foldable empty-string argument.
class EmptyNeedleRemovalRule : public Rule {
public:
	explicit EmptyNeedleRemovalRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
