//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_aggregate_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression.hpp"

namespace duckdb {

class BoundAggregateExpression : public Expression {
public:
	BoundAggregateExpression(TypeId return_type, ExpressionType type, unique_ptr<Expression> child);

	//! The child of the aggregate expression
	unique_ptr<Expression> child;

public:
	bool IsAggregate() const override {
		return true;
	}
	bool IsFoldable() const override {
		return false;
	}

	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
