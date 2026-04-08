//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/column_binding_replacer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class Expression;
class LogicalOperator;

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
	vector<ReplacementBinding> replacement_bindings;

	//! Do not recurse further than this operator (optional)
	optional_ptr<LogicalOperator> stop_operator;
};

} // namespace duckdb
