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

enum WindowBoundary { INVALID, UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING, CURRENT_ROW, EXPR_PRECEDING, EXPR_FOLLOWING };

class WindowExpression : public Expression {
public:
	WindowExpression(ExpressionType type, unique_ptr<Expression> child);

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::WINDOW;
	}

	void ResolveType() override;

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

	WindowBoundary start = WindowBoundary::INVALID, end = WindowBoundary::INVALID;
	unique_ptr<Expression> start_expr = nullptr, end_expr = nullptr;

private:
};
} // namespace duckdb
