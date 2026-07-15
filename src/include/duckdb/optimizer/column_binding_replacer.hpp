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

class BoundColumnRefExpression;
class BoundSubqueryExpression;

struct ReplacementBinding {
public:
	ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding);
	ReplacementBinding(ColumnBinding old_binding, ColumnBinding new_binding, LogicalType new_type);

public:
	ColumnBinding old_binding;
	ColumnBinding new_binding;

	bool replace_type;
	LogicalType new_type;
};

//! The ColumnBindingReplacer updates column bindings (e.g., after changing the operator plan), utility for optimizers
class ColumnBindingReplacer : public LogicalOperatorVisitor {
public:
	ColumnBindingReplacer();

public:
	//! Update each operator of the plan
	void VisitOperator(LogicalOperator &op) override;
	//! Update bindings owned by this operator without visiting its children
	virtual void VisitOperatorBindings(LogicalOperator &op);
	//! Add a binding replacement
	void AddReplacement(ColumnBinding old_binding, ColumnBinding new_binding);
	//! Add binding replacements by position
	void AddReplacements(const vector<ColumnBinding> &old_bindings, const vector<ColumnBinding> &new_bindings);

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override;

public:
	//! Do not recurse further than this operator (optional)
	optional_ptr<LogicalOperator> stop_operator;

	//! Contains all bindings that need to be updated
	vector<ReplacementBinding> replacement_bindings;
};

//! Like ColumnBindingReplacer, but also updates correlated-column metadata and nested subquery plans.
class CorrelatedColumnBindingReplacer : public ColumnBindingReplacer {
public:
	void VisitOperatorBindings(LogicalOperator &op) override;

protected:
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;
};

} // namespace duckdb
