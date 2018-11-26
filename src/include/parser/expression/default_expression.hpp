//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/default_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! Represents the default value of a column
class DefaultExpression : public Expression {
  public:
	DefaultExpression() : Expression(ExpressionType::VALUE_DEFAULT) {
	}

	std::unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::DEFAULT;
	}

	std::unique_ptr<Expression> Copy() override;

	//! Deserializes a blob back into an DefaultExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	std::string ToString() const override {
		return "Default";
	}
};
} // namespace duckdb
