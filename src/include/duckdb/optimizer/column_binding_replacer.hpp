//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/column_binding_replacer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

struct ReplaceBinding {
public:
	ReplaceBinding();
	ReplaceBinding(ColumnBinding old_binding, ColumnBinding new_binding);
	ReplaceBinding(ColumnBinding old_binding, ColumnBinding new_binding, LogicalType new_type);

public:
	ColumnBinding old_binding;
	ColumnBinding new_binding;

	bool replace_type = false;
	LogicalType new_type;
};

typedef void (*column_binding_callback_t)(BoundColumnRefExpression &bound_column_ref,
                                          const ReplaceBinding &replace_binding);

//! The ColumnBindingReplacer updates column bindings (e.g., after changing the operator plan), utility for optimizers
class ColumnBindingReplacer : LogicalOperatorVisitor {
public:
	ColumnBindingReplacer();

	//! Update each operator of the plan
	void VisitOperator(LogicalOperator &op) override;
	//! Visit an expression and update its column bindings
	void VisitExpression(unique_ptr<Expression> *expression) override;

	//! Contains all bindings that need to be updated
	vector<ReplaceBinding> replace_bindings;

	//! Do not recurse further than this operator (optional)
	LogicalOperator *stop_operator = nullptr;

	//! Extra callback (optional)
	column_binding_callback_t column_binding_callback = nullptr;
};

} // namespace duckdb
