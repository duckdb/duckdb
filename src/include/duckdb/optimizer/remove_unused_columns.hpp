//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/remove_unused_columns.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class BoundColumnRefExpression;

//! The RemoveUnusedColumns optimizer traverses the logical operator tree and removes any columns that are not required
class RemoveUnusedColumns : public LogicalOperatorVisitor {
public:
	RemoveUnusedColumns(bool is_root = false) : everything_referenced(is_root) {}

	void VisitOperator(LogicalOperator &op) override;
protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) override;
private:
	//! Whether or not all the columns are referenced. This happens in the case of the root expression (because the output implicitly refers all the columns below it)
	bool everything_referenced;
	//! The map of column references
	unordered_map<index_t, unordered_map<index_t, vector<BoundColumnRefExpression*>>> column_references;
private:
	void ClearExpressions(LogicalOperator &op, unordered_map<index_t, vector<BoundColumnRefExpression*>> &ref_map);
};
} // namespace duckdb
