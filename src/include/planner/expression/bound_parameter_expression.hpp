//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_parameter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression.hpp"

namespace duckdb {

class BoundParameterExpression : public Expression {
public:
	BoundParameterExpression(TypeId type, size_t parameter_nr) : 
		Expression(ExpressionType::VALUE_PARAMETER, ExpressionClass::BOUND_PARAMETER, TypeId::INVALID),
		parameter_nr(parameter_nr) {}

	size_t parameter_nr;
public:
	bool IsScalar() const override {
		return true;
	}
	bool HasParameter() const override {
		return true;
	}

	string ToString() const override {
		return to_string(parameter_nr);
	}

	unique_ptr<Expression> Copy() override {
		return make_unique<BoundParameterExpression>(return_type, parameter_nr);
	}
};
} // namespace duckdb
