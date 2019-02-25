//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/default_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {
//! Represents the default value of a column
class DefaultExpression : public Expression {
public:
	DefaultExpression() : Expression(ExpressionType::VALUE_DEFAULT) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::DEFAULT;
	}

	unique_ptr<Expression> Copy() override;

	//! Deserializes a blob back into an DefaultExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	bool IsScalar() override {
		return false;
	}

	string ToString() const override {
		return "DEFAULT";
	}
};
} // namespace duckdb
