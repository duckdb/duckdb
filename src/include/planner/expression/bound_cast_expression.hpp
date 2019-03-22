//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_cast_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression.hpp"

namespace duckdb {

class BoundCastExpression : public Expression {
public:
	BoundCastExpression(TypeId target, SQLType target_type, unique_ptr<Expression> child);

	//! The child type
	unique_ptr<Expression> child;

public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
