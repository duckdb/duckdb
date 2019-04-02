//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/expression/window_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/parsed_expression.hpp"
#include "parser/query_node.hpp"

namespace duckdb {

enum class WindowBoundary : uint8_t {
	INVALID = 0,
	UNBOUNDED_PRECEDING = 1,
	UNBOUNDED_FOLLOWING = 2,
	CURRENT_ROW_RANGE = 3,
	CURRENT_ROW_ROWS = 4,
	EXPR_PRECEDING = 5,
	EXPR_FOLLOWING = 6
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
	vector<OrderByNode> orders;
	//! The window boundaries
	WindowBoundary start = WindowBoundary::INVALID;
	WindowBoundary end = WindowBoundary::INVALID;

	unique_ptr<ParsedExpression> start_expr;
	unique_ptr<ParsedExpression> end_expr;
	//! Offset and default expressions for WINDOW_LEAD and WINDOW_LAG functions
	unique_ptr<ParsedExpression> offset_expr;
	unique_ptr<ParsedExpression> default_expr;

public:
	bool IsWindow() const override {
		return true;
	}

	string ToString() const override;

	bool Equals(const BaseExpression *other) const override;

	unique_ptr<ParsedExpression> Copy() const override;

	void Serialize(Serializer &serializer) override;
	static unique_ptr<ParsedExpression> Deserialize(ExpressionType type, Deserializer &source);
};
} // namespace duckdb
