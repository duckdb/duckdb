//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_conjunction_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundConjunctionExpression : public Expression {
public:
	BoundConjunctionExpression(ExpressionType type);
	BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right);

	vector<unique_ptr<Expression>> children;

public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
