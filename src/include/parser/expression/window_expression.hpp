//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/window_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "parser/expression.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/query_node.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {
//! The WindowExpression represents a window function in the query. They are a special case of aggregates which is why
//! they inherit from them.
class WindowExpression : public AggregateExpression {
public:
	WindowExpression(ExpressionType type, unique_ptr<Expression> child);

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::WINDOW;
	}

	bool IsAggregate() override {
		// fixme this is dirty, perhaps this should not inherit from aggregateexpression after all
		return false;
	}

	unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}

	unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static unique_ptr<Expression> Deserialize(ExpressionDeserializeInfo *info, Deserializer &source);

	vector<unique_ptr<Expression>> partitions;
	OrderByDescription ordering;

private:
};
} // namespace duckdb
