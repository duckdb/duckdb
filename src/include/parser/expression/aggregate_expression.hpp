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
#include "parser/sql_node_visitor.hpp"

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

	unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}
	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::AGGREGATE;
	}

	unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an AggregateExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);

	string ToString() const override {
		return GetName() + "(" + child->ToString() + ")";
	}

	string GetName() const override;

	bool Equals(const Expression *other) const override;

	size_t ChildCount() const override;
	Expression *GetChild(size_t index) const override;
	void ReplaceChild(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                  size_t index) override;

	size_t index;
	//! The child of the aggregate expression
	unique_ptr<Expression> child;
};
} // namespace duckdb
