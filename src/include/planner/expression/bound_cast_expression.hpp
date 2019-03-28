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
	BoundCastExpression(TypeId target, unique_ptr<Expression> child, SQLType target_type = SQLType());

	static unique_ptr<Expression> AddCastToType(TypeId target_type, unique_ptr<Expression> expr);

	//! The child type
	unique_ptr<Expression> child;
public:
	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<Expression> Copy() override;
};
} // namespace duckdb
