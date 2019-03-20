//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statement/select_statement.hpp"
#include "parser/tableref.hpp"

namespace duckdb {

//! Represents a subquery
class SubqueryExpression : public Expression {
public:
	SubqueryExpression()
	    : Expression(ExpressionType::SUBQUERY), subquery_type(SubqueryType::INVALID),
	      comparison_type(ExpressionType::INVALID) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::SUBQUERY;
	}

	unique_ptr<Expression> Copy() const override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	bool Equals(const Expression *other) const override;

	//! The actual subquery
	unique_ptr<QueryNode> subquery;
	//! The subquery type
	SubqueryType subquery_type;
	//! the child expression to compare with (in case of IN, ANY, ALL operators, empy for EXISTS queries and scalar
	//! subquery)
	unique_ptr<Expression> child;
	//! The comparison type of the child expression with the subquery (in case of ANY, ALL operators), empty otherwise
	ExpressionType comparison_type;

	size_t ChildCount() const override;
	Expression *GetChild(size_t index) const override;
	void ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                  size_t index) override;

	string ToString() const override {
		return "SUBQUERY";
	}

	bool HasSubquery() override {
		return true;
	}

	bool IsScalar() override {
		return false;
	}
};
} // namespace duckdb
