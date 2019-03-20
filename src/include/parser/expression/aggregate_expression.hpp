//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/aggregate_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "parser/expression.hpp"

namespace duckdb {
//! The AggregateExpression represents an aggregate in the query
class AggregateExpression : public Expression {
public:
	AggregateExpression(ExpressionType type, unique_ptr<Expression> child);

	//! Resolve the type of the aggregate
	void ResolveType() override;

	bool IsAggregate() override {
		return true;
	}

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::AGGREGATE;
	}

	unique_ptr<Expression> Copy() const override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an AggregateExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	string ToString() const override {
		if (type ==  ExpressionType::AGGREGATE_COUNT_STAR) {
			return GetName() + "(*)";
		}

		return GetName() + "(" + child->ToString() + ")";
	}

	string GetName() const override;

	bool Equals(const Expression *other) const override;

	size_t ChildCount() const override;
	Expression *GetChild(size_t index) const override;
	void ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                  size_t index) override;

	//! The child of the aggregate expression
	unique_ptr<Expression> child;
};
} // namespace duckdb
