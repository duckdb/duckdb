//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/window_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/query_node.hpp"

namespace duckdb {

enum WindowBoundary {
	INVALID,
	UNBOUNDED_PRECEDING,
	UNBOUNDED_FOLLOWING,
	CURRENT_ROW_RANGE,
	CURRENT_ROW_ROWS,
	EXPR_PRECEDING,
	EXPR_FOLLOWING
};

//! The WindowExpression represents a window function in the query. They are a special case of aggregates which is why
//! they inherit from them.
class WindowExpression : public ParsedExpression {
public:
	WindowExpression(ExpressionType type, unique_ptr<ParsedExpression> child);

	//! The child expression of the main window aggregate
	unique_ptr<ParsedExpression> child;
	//! The set of expressions to partition by
	vector<unique_ptr<ParsedExpression>> partitions;
	//! The set of ordering clauses
	OrderByDescription ordering;
	//! The window boundaries
	WindowBoundary start = WindowBoundary::INVALID, end = WindowBoundary::INVALID;
	unique_ptr<ParsedExpression> start_expr = nullptr, end_expr = nullptr;
	//! Offset and default expressions for WINDOW_LEAD and WINDOW_LAG functions
	unique_ptr<ParsedExpression> offset_expr = nullptr, default_expr = nullptr;
public:
	bool IsWindow() override {
		return true;
	}

	string ToString() const override;

	bool Equals(const ParsedExpression *other) const override;
	
	unique_ptr<ParsedExpression> Copy() override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);

	size_t ChildCount() const override;
	ParsedExpression *GetChild(size_t index) const override;
	void ReplaceChild(std::function<unique_ptr<ParsedExpression>(unique_ptr<ParsedExpression> expression)> callback,
	                  size_t index) override;
};
} // namespace duckdb
