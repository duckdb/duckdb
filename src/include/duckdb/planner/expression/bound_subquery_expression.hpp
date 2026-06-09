//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundSubqueryExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_SUBQUERY;

public:
	explicit BoundSubqueryExpression(LogicalType return_type);

	bool IsCorrelated() const {
		return !binder->correlated_columns.empty();
	}

	//! The binder used to bind the subquery node
	shared_ptr<Binder> binder;
	//! The bound subquery node
	BoundStatement subquery;
	//! The subquery type
	SubqueryType subquery_type;
	//! the child expressions to compare with (in case of IN, ANY, ALL operators)
	vector<unique_ptr<Expression>> children;
	//! The comparison type of the child expression with the subquery (in case of ANY, ALL operators)
	ExpressionType comparison_type;
	//! The LogicalTypes of the subquery result. Only used for ANY expressions.
	vector<LogicalType> child_types;
	//! The target LogicalType of the subquery result (i.e. to which type it should be casted, if child_type <>
	//! child_target). Only used for ANY expressions.
	vector<LogicalType> child_targets;

public:
	bool HasSubquery() const override {
		return true;
	}
	bool IsScalar() const override {
		return false;
	}
	bool IsFoldable() const override {
		return false;
	}

	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;

	bool PropagatesNullValues() const override;
};
} // namespace duckdb
