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

enum WindowBoundary {
	INVALID,
	UNBOUNDED_PRECEDING,
	UNBOUNDED_FOLLOWING,
	CURRENT_ROW_RANGE,
	CURRENT_ROW_ROWS,
	EXPR_PRECEDING,
	EXPR_FOLLOWING
};

class WindowExpression : public Expression {
public:
	WindowExpression(ExpressionType type, unique_ptr<Expression> child);

	ExpressionClass GetExpressionClass() override {
		return ExpressionClass::WINDOW;
	}

	void ResolveType() override;

	bool IsWindow() override {
		return true;
	}

	unique_ptr<Expression> Accept(SQLNodeVisitor *v) override {
		return v->Visit(*this);
	}

	unique_ptr<Expression> Copy() override;

	void EnumerateChildren(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback) override;
	void EnumerateChildren(std::function<void(Expression* expression)> callback) const override;

	//! Serializes an WindowExpression to a stand-alone binary blob
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an WindowExpression
	static unique_ptr<Expression> Deserialize(ExpressionType type, TypeId return_type, Deserializer &source);
	bool Equals(const Expression *other) const override;

	string ToString() const override {
		return "WINDOW";
	}

	//! The child expression of the main window aggregate
	unique_ptr<Expression> child;
	//! The set of expressions to partition by
	vector<unique_ptr<Expression>> partitions;
	//! The set of ordering clauses
	OrderByDescription ordering;
	//! The window boundaries
	WindowBoundary start = WindowBoundary::INVALID, end = WindowBoundary::INVALID;
	unique_ptr<Expression> start_expr = nullptr, end_expr = nullptr;
	//! Offset and default expressions for WINDOW_LEAD and WINDOW_LAG functions
	unique_ptr<Expression> offset_expr = nullptr, default_expr = nullptr;

private:
};
} // namespace duckdb
