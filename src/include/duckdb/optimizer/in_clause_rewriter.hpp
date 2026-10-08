//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/in_clause_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class InClauseRewriter : public LogicalOperatorVisitor {
public:
	//! Minimum number of children (including the probe expression) to evaluate an IN with a hash lookup
	static constexpr idx_t IN_CLAUSE_REWRITE_THRESHOLD = 6;
	//! Whether the IN is evaluated with a hash lookup of its constant values
	static bool UsesHashLookup(const BoundOperatorExpression &expr);
	//! Whether the expression contains an IN that is evaluated with a hash lookup
	static bool HasRewritableInClause(const Expression &expr);
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);
	unique_ptr<Expression> VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) override;
};

} // namespace duckdb
