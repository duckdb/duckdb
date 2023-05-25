//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_elimination.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class JoinElimination : public LogicalOperatorVisitor {
public:
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	//! The set of column references
	column_binding_set_t column_references;
	//! The set of unique column references
	column_binding_set_t unique_column_references;
};
} // namespace duckdb
