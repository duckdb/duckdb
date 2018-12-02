//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/expression/star_expression.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

//! Represents a * expression in the SELECT clause
class StarExpression : public Expression {
  public:
	StarExpression() : Expression(ExpressionType::STAR) {
	}

	std::unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::STAR;
	}

	std::unique_ptr<Expression> Copy() override;

	//! Deserializes a blob back into a StarExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	std::string ToString() const override {
		return "*";
	}
};
} // namespace duckdb
