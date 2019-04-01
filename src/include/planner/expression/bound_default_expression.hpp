//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_default_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression.hpp"

namespace duckdb {

class BoundDefaultExpression : public Expression {
public:
	BoundDefaultExpression(TypeId type)
	    : Expression(ExpressionType::VALUE_DEFAULT, ExpressionClass::BOUND_DEFAULT, type) {
	}

public:
	string ToString() const override {
		return "DEFAULT";
	}

	unique_ptr<Expression> Copy() override {
		return make_unique<BoundDefaultExpression>(return_type);
	}
};
} // namespace duckdb
