//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/bound_subquery_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/subquery_expression.hpp"
#include "planner/binder.hpp"

namespace duckdb {
class LogicalOperator;

//! Represents a subquery
class BoundSubqueryExpression : public Expression {
public:
	BoundSubqueryExpression() : Expression(ExpressionType::SUBQUERY) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::BOUND_SUBQUERY;
	}

	unique_ptr<Expression> Copy() const override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;

	bool Equals(const Expression *other) const override;

	unique_ptr<Binder> binder;
	unique_ptr<Expression> subquery;

	bool IsCorrelated() {
		return binder->correlated_columns.size() > 0;
	}

	size_t ChildCount() const override {
		return subquery->ChildCount();
	}
	Expression *GetChild(size_t index) const override {
		return subquery->GetChild(index);
	}
	void ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                  size_t index) override {
		return subquery->ReplaceChild(callback, index);
	}

	string ToString() const override {
		return subquery->ToString();
	}

	bool HasSubquery() override {
		return true;
	}

	bool IsScalar() override {
		return false;
	}
};
} // namespace duckdb
