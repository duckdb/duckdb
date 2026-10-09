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
	//! Whether the expression contains a large constant IN that is kept as an IN
	static bool HasRewritableInClause(const Expression &expr);
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);
	unique_ptr<Expression> VisitReplace(BoundOperatorExpression &expr, unique_ptr<Expression> *expr_ptr) override;
};

} // namespace duckdb
