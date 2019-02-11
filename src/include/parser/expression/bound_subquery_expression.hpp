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

struct CorrelatedColumnInfo {
       ColumnBinding binding;
       TypeId type;
       string name;

       CorrelatedColumnInfo(BoundColumnRefExpression& expr) {
               binding = expr.binding;
               type = expr.return_type;
               name = expr.GetName();
       }

       bool operator==(const CorrelatedColumnInfo &rhs) const {
               return binding == rhs.binding;
       }
};

//! Represents a subquery
class BoundSubqueryExpression : public Expression {
public:
	BoundSubqueryExpression(ClientContext &context) : Expression(ExpressionType::SUBQUERY), binder(context) {
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::BOUND_SUBQUERY;
	}

	unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;

	bool Equals(const Expression *other) const override;

	Binder binder;
	unique_ptr<Expression> subquery;
	vector<CorrelatedColumnInfo> correlated_columns;

	bool IsCorrelated() {
		return correlated_columns.size() > 0;
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
