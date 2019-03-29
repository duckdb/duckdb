//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression/bound_parameter_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "planner/expression.hpp"

namespace duckdb {

class BoundParameterExpression : public Expression {
public:
	BoundParameterExpression(size_t parameter_nr)
	    : Expression(ExpressionType::VALUE_PARAMETER, ExpressionClass::BOUND_PARAMETER, TypeId::INVALID, SQLType()),
	      parameter_nr(parameter_nr), value(nullptr) {
	}

	size_t parameter_nr;
	Value *value;
public:
	bool IsScalar() const override {
		return true;
	}
	bool HasParameter() const override {
		return true;
	}

	string ToString() const override {
		return std::to_string(parameter_nr);
	}

	unique_ptr<Expression> Copy() override {
		return make_unique<BoundParameterExpression>(parameter_nr);
	}
};
} // namespace duckdb
