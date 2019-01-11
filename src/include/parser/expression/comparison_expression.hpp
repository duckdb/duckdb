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
	    : Expression(type, TypeId::BOOLEAN) {
		this->left = move(left);
		this->right = move(right);
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::COMPARISON;
	}

	unique_ptr<Expression> Copy() override;

	size_t ChildCount() const override;
	Expression *GetChild(size_t index) const override;
	void ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                  size_t index) override;

	//! Serializes a CastExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ComparisonExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	bool Equals(const Expression *other) const override;

	string ToString() const override {
		return left->ToString() + ExpressionTypeToOperator(type) + right->ToString();
	}

	static ExpressionType NegateComparisionExpression(ExpressionType type);
	static ExpressionType FlipComparisionExpression(ExpressionType type);

	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
};
} // namespace duckdb
