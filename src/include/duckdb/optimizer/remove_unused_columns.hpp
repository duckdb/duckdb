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

struct ReferencedExtractComponent {
public:
	explicit ReferencedExtractComponent(unique_ptr<Expression> &extract) : extract(extract) {
	}

public:
	//! The extract expression in the chain of extracts (i.e: s.my_field.my_nested_field has 2 components)
	unique_ptr<Expression> &extract;
	//! (Optionally) the cast on top of the extract
	optional_ptr<unique_ptr<Expression>> cast;
};

struct ReferencedStructExtract {
public:
	ReferencedStructExtract(vector<ReferencedExtractComponent> components, idx_t bindings_idx, ColumnIndex &&path)
	    : bindings_idx(bindings_idx), components(std::move(components)), extract_path(std::move(path)) {
	}

public:
	//! The index into the 'bindings' of the ReferencedColumn that is the child of this struct_extract
	idx_t bindings_idx;
	//! The struct extract components, in order from root to leaf
	vector<ReferencedExtractComponent> components;
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
	                const vector<ReferencedExtractComponent> &parent);
	//! Perform a replacement of the ColumnBinding, iterating over all the currently found column references and
	//! replacing the bindings
	//! ret: The amount of bindings created
	idx_t ReplaceBinding(ColumnBinding current_binding, ColumnBinding new_binding);

	bool HandleExtractExpression(unique_ptr<Expression> *expression,
	                             optional_ptr<unique_ptr<Expression>> cast_expression = nullptr);

	bool HandleStructExtract(unique_ptr<Expression> &expr, optional_ptr<BoundColumnRefExpression> &colref,
	                         reference<ColumnIndex> &path_ref, vector<ReferencedExtractComponent> &expressions);
	bool HandleVariantExtract(unique_ptr<Expression> &expr, optional_ptr<BoundColumnRefExpression> &colref,
	                          reference<ColumnIndex> &path_ref, vector<ReferencedExtractComponent> &expressions);
	bool HandleExtractRecursive(unique_ptr<Expression> &expr, optional_ptr<BoundColumnRefExpression> &colref,
	                            reference<ColumnIndex> &path_ref, vector<ReferencedExtractComponent> &expressions);
	void SetMode(BaseColumnPrunerMode mode);
	BaseColumnPrunerMode GetMode() const;

private:
	void MergeChildColumns(vector<ColumnIndex> &current_child_columns, ColumnIndex &new_child_column);

protected:
	//! The map of column references
	column_binding_map_t<ReferencedColumn> column_references;

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
	void CheckPushdownExtract(LogicalOperator &op);
	void RewriteExpressions(LogicalProjection &proj, idx_t expression_count);
	void WritePushdownExtractColumns(
	    const ColumnBinding &binding, ReferencedColumn &col, idx_t original_idx, const LogicalType &column_type,
	    const std::function<idx_t(const ColumnIndex &new_index, optional_ptr<const LogicalType> cast_type)> &callback);
};
} // namespace duckdb
