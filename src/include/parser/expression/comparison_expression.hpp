//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/comparison_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! Represents a boolean comparison (e.g. =, >=, <>). Always returns a boolean
//! and has two children.
class ComparisonExpression : public Expression {
public:
	ComparisonExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right)
	    : Expression(type, TypeId::BOOLEAN, std::move(left), std::move(right)) {
	}

	unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::COMPARISON;
	}

	unique_ptr<Expression> Copy() override;

	//! Deserializes a blob back into an OperatorExpression
	static unique_ptr<Expression> Deserialize(ExpressionDeserializeInfo *info, Deserializer &source);
	static ExpressionType NegateComparisionExpression(ExpressionType type);
	static ExpressionType FlipComparisionExpression(ExpressionType type);
};
} // namespace duckdb
