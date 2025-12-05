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
#include "duckdb/common/column_index_map.hpp"

namespace duckdb {
class Binder;
class BoundColumnRefExpression;
class ClientContext;

struct ReferencedStructExtract {
public:
	ReferencedStructExtract(vector<reference<unique_ptr<Expression>>> expressions, idx_t bindings_idx,
	                        ColumnIndex &&path)
	    : bindings_idx(bindings_idx), expr(expressions), extract_path(std::move(path)) {
	}

public:
	//! The index into the 'bindings' of the ReferencedColumn that is the child of this struct_extract
	idx_t bindings_idx;
	//! The struct extract expressions, from deepest (most nested) to the root (i.e:
	//! s.my_field.my_nested_field.my_even_deeper_nested_field is index 0)
	vector<reference<unique_ptr<Expression>>> expr;
	//! The ColumnIndex with a path that matches this struct extract
	ColumnIndex extract_path;
};

enum class PushdownExtractSupport : uint8_t { UNCHECKED, DISABLED, ENABLED };

class ReferencedColumn {
public:
	void AddPath(const ColumnIndex &path);

public:
	//! The BoundColumnRefExpressions in the operator that reference the same ColumnBinding
	vector<reference<BoundColumnRefExpression>> bindings;
	vector<ReferencedStructExtract> struct_extracts;
	vector<ColumnIndex> child_columns;
	//! Whether we can create a pushdown extract for the children of this column (if any)
	PushdownExtractSupport supports_pushdown_extract = PushdownExtractSupport::UNCHECKED;
	//! Map from extract path to the binding created for it (if pushdown extract)
	column_index_set unique_paths;
};

enum class BaseColumnPrunerMode : uint8_t {
	DEFAULT,
	//! Any child reference disables PUSHDOWN_EXTRACT for the parent column
	DISABLE_PUSHDOWN_EXTRACT
};

class BaseColumnPruner : public LogicalOperatorVisitor {
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
	                vector<reference<unique_ptr<Expression>>> parent);
	//! Perform a replacement of the ColumnBinding, iterating over all the currently found column references and
	//! replacing the bindings
	//! ret: The amount of bindings created
	idx_t ReplaceBinding(ColumnBinding current_binding, ColumnBinding new_binding);

	bool HandleStructExtract(unique_ptr<Expression> *expression);

	bool HandleStructExtractRecursive(unique_ptr<Expression> &expr, optional_ptr<BoundColumnRefExpression> &colref,
	                                  vector<idx_t> &indexes, vector<reference<unique_ptr<Expression>>> &expressions);
	void SetMode(BaseColumnPrunerMode mode);
	bool HandleStructPack(Expression &expr);
	BaseColumnPrunerMode GetMode() const;

private:
	void MergeChildColumns(vector<ColumnIndex> &current_child_columns, ColumnIndex &new_child_column);

protected:
	//! The map of column references
	column_binding_map_t<ReferencedColumn> column_references;
	vector<ColumnIndex> deliver_child;

private:
	//! The current mode of the pruner, enables/disables certain behaviors
	BaseColumnPrunerMode mode = BaseColumnPrunerMode::DEFAULT;
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
	void CheckPushdownExtract(LogicalGet &get);
};
} // namespace duckdb
