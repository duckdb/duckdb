//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/conjunction_expression.hpp
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
	ConjunctionExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right)
	    : Expression(type, TypeId::BOOLEAN) {
		this->left = move(left);
		this->right = move(right);
	}

	void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::CONJUNCTION;
	}

	unique_ptr<Expression> Copy() override;

	size_t ChildCount() const override;
	Expression *GetChild(size_t index) const override;
	void ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                  size_t index) override;

	//! Serializes a ConjunctionExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a ConjunctionExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	bool Equals(const Expression *other) const override;

	string ToString() const override {
		return left->ToString() + " " + ExpressionTypeToOperator(type) + " " + right->ToString();
	}

	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
};
} // namespace duckdb
