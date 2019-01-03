//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/star_expression.hpp
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

	unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::STAR;
	}

	unique_ptr<Expression> Copy() override;

	void EnumerateChildren(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) override {}
	void EnumerateChildren(std::function<void(Expression* expression)> callback) const override {}
	//! Deserializes a blob back into a StarExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	string ToString() const override {
		return "*";
	}
};
} // namespace duckdb
