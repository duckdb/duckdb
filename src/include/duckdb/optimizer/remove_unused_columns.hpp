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
#include "duckdb/common/column_index.hpp"

namespace duckdb {
class Binder;
class BoundColumnRefExpression;
class ClientContext;

struct ReferencedColumn {
	vector<reference<BoundColumnRefExpression>> bindings;
	vector<ColumnIndex> child_columns;
};

class BaseColumnPruner : public LogicalOperatorVisitor {
protected:
	//! The map of column references
	column_binding_map_t<ReferencedColumn> column_references;

protected:
	void VisitExpression(unique_ptr<Expression> *expression) override;

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) override;

protected:
	//! Add a reference to the column in its entirey
	void AddBinding(BoundColumnRefExpression &col);
	//! Add a reference to a sub-section of the column
	void AddBinding(BoundColumnRefExpression &col, ColumnIndex child_column);
	//! Perform a replacement of the ColumnBinding, iterating over all the currently found column references and
	//! replacing the bindings
	void ReplaceBinding(ColumnBinding current_binding, ColumnBinding new_binding);

	bool HandleStructExtract(Expression &expr);

	bool HandleStructExtractRecursive(Expression &expr, optional_ptr<BoundColumnRefExpression> &colref,
	                                  vector<idx_t> &indexes);
};

//! The RemoveUnusedColumns optimizer traverses the logical operator tree and removes any columns that are not required
class RemoveUnusedColumns : public BaseColumnPruner {
public:
	RemoveUnusedColumns(Binder &binder, ClientContext &context, bool is_root = false)
	    : binder(binder), context(context), everything_referenced(is_root) {
	}

	void VisitOperator(LogicalOperator &op) override;

private:
	Binder &binder;
	ClientContext &context;
	//! Whether or not all the columns are referenced. This happens in the case of the root expression (because the
	//! output implicitly refers all the columns below it)
	bool everything_referenced;

private:
	template <class T>
	void ClearUnusedExpressions(vector<T> &list, idx_t table_idx, bool replace = true);
	void RemoveColumnsFromLogicalGet(LogicalGet &get);
};
} // namespace duckdb
