//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/equal_or_null_simplification.hpp
//
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

// Rewrite
// a=b OR (a IS NULL AND b IS NULL) to a IS NOT DISTINCT FROM b
class EqualOrNullSimplification : public Rule {
public:
	explicit EqualOrNullSimplification(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
