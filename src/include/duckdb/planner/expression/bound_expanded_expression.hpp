//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_expanded_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! BoundExpression is an intermediate dummy expression used by the binder.
//! It holds a set of expressions that will be "expanded" in the select list of a query
class BoundExpandedExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_EXPANDED;

public:
	explicit BoundExpandedExpression(vector<unique_ptr<Expression>> expanded_expressions);

	vector<unique_ptr<Expression>> expanded_expressions;

public:
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;
};

} // namespace duckdb
