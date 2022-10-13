//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/remove_unused_columns.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class Binder;
class BoundColumnRefExpression;
class ClientContext;

//! The RemoveUnusedColumns optimizer traverses the logical operator tree and removes any columns that are not required
class RemoveUnusedColumns : public LogicalOperatorVisitor {
public:
	RemoveUnusedColumns(Binder &binder, ClientContext &context, bool is_root = false)
	    : binder(binder), context(context), everything_referenced(is_root) {
	}

	void VisitOperator(LogicalOperator &op) override;

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) override;

private:
	Binder &binder;
	ClientContext &context;
	//! Whether or not all the columns are referenced. This happens in the case of the root expression (because the
	//! output implicitly refers all the columns below it)
	bool everything_referenced;
	//! The map of column references
	column_binding_map_t<vector<BoundColumnRefExpression *>> column_references;

private:
	template <class T>
	void ClearUnusedExpressions(vector<T> &list, idx_t table_idx, bool replace = true);

	//! Perform a replacement of the ColumnBinding, iterating over all the currently found column references and
	//! replacing the bindings
	void ReplaceBinding(ColumnBinding current_binding, ColumnBinding new_binding);
};
} // namespace duckdb
