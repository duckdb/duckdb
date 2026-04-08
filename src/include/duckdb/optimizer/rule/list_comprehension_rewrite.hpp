//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/list_comprehension_rewrite.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ExpressionRewriter;
class LogicalOperator;

//! Rewrites list comprehensions that use struct_pack for filter/result into list_filter + list_apply
class ListComprehensionRewriteRule : public Rule {
public:
	explicit ListComprehensionRewriteRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &fixed_point,
	                             bool is_root) override;
};

} // namespace duckdb
