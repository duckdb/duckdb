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

	bool replace_type;
	LogicalType new_type;
};

//! The ColumnBindingReplacer updates column bindings (e.g., after changing the operator plan), utility for optimizers
class ColumnBindingReplacer : LogicalOperatorVisitor {
public:
	ColumnBindingReplacer();

public:
	//! Update each operator of the plan
	void VisitOperator(LogicalOperator &op) override;
	//! Visit an expression and update its column bindings
	void VisitExpression(unique_ptr<Expression> *expression) override;

public:
	//! Contains all bindings that need to be updated
	vector<ReplaceBinding> replace_bindings;

	//! Do not recurse further than this operator (optional)
	LogicalOperator *stop_operator = nullptr;
	//! Extra callback (optional)
	std::function<void(BoundColumnRefExpression &, const ReplaceBinding &)> column_binding_callback = nullptr;
};

} // namespace duckdb
