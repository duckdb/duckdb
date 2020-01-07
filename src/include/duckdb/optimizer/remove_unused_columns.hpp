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

namespace duckdb {
class BoundColumnRefExpression;

//! The RemoveUnusedColumns optimizer traverses the logical operator tree and removes any columns that are not required
class RemoveUnusedColumns : public LogicalOperatorVisitor {
public:
	RemoveUnusedColumns(bool is_root = false) : everything_referenced(is_root) {}
	~RemoveUnusedColumns();

	void VisitOperator(LogicalOperator &op) override;
protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;
	unique_ptr<Expression> VisitReplace(BoundReferenceExpression &expr, unique_ptr<Expression> *expr_ptr) override;
private:
	//! Whether or not all the columns are referenced. This happens in the case of the root expression (because the output implicitly refers all the columns below it)
	bool everything_referenced;
	//! The map of column references
	column_binding_map_t<vector<BoundColumnRefExpression*>> column_references;
	//! Map of original column binding -> updated column binding
	column_binding_map_t<ColumnBinding> remap;
	//! Map of updated column binding -> original column binding
	column_binding_map_t<ColumnBinding> original_bindings;
private:
	template<class T>
	void ClearUnusedExpressions(vector<T> &list, index_t table_idx);
};
} // namespace duckdb
