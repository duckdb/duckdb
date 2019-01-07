//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/case_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

class CaseExpression : public Expression {
public:
	// this expression has 3 children, (1) the check, (2) the result if the test is true, and (3) the result if the test
	// is false
	CaseExpression() : Expression(ExpressionType::OPERATOR_CASE_EXPR) {
	}

	unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::CASE;
	}

	unique_ptr<Expression> Copy() override;

	void EnumerateChildren(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) override;
	void EnumerateChildren(std::function<void(Expression *expression)> callback) const override;

	//! Serializes a CaseExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an CaseExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	bool Equals(const Expression *other) const override;

	void ResolveType() override;

	string ToString() const override {
		return "CASE WHEN (" + check->ToString() + ") THEN (" + result_if_true->ToString() + ") ELSE (" +
		       result_if_false->ToString() + ")";
	}

	unique_ptr<Expression> check;
	unique_ptr<Expression> result_if_true;
	unique_ptr<Expression> result_if_false;
};
} // namespace duckdb
