//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// parser/expression/conjunction_expression.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! Represents a conjunction (AND/OR)
class ConjunctionExpression : public Expression {
  public:
	ConjunctionExpression(ExpressionType type, std::unique_ptr<Expression> left,
	                      std::unique_ptr<Expression> right)
	    : Expression(type, TypeId::BOOLEAN, std::move(left), std::move(right)) {
	}

	std::unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::CONJUNCTION;
	}

	std::unique_ptr<Expression> Copy() override;

	bool Equals(const Expression *other) override;

	//! Deserializes a blob back into a ConjunctionExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInfo *info, Deserializer &source);
};
} // namespace duckdb
