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

struct ReferencedStructExtract {
public:
	ReferencedStructExtract(optional_ptr<unique_ptr<Expression>> struct_extract, idx_t bindings_idx)
	    : bindings_idx(bindings_idx), expr(struct_extract) {
	}

public:
	//! The index into the 'bindings' of the ReferencedColumn that is the child of this struct_extract
	idx_t bindings_idx;
	//! The struct_extract expression to potentially replace
	optional_ptr<unique_ptr<Expression>> expr;
};

struct ReferencedColumn {
	//! The BoundColumnRefExpressions in the operator that reference the same ColumnBinding
	vector<reference<BoundColumnRefExpression>> bindings;
	vector<ReferencedStructExtract> struct_extracts;
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
	//! Add a reference to a sub-section of the column used in a struct extract, with the parent expression
	void AddBinding(BoundColumnRefExpression &col, ColumnIndex child_column,
	                optional_ptr<unique_ptr<Expression>> parent);
	//! Perform a replacement of the ColumnBinding, iterating over all the currently found column references and
	//! replacing the bindings
	void ReplaceBinding(ColumnBinding current_binding, ColumnBinding new_binding);

	bool HandleStructExtract(unique_ptr<Expression> *expression);

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
	void AdjustFilters(vector<unique_ptr<Expression>> &expressions, map<idx_t, unique_ptr<TableFilter>> &filters);
};
} // namespace duckdb
