#pragma once
#include "duckdb/parser/expression/window_expression.hpp"

namespace duckdb {

struct WindowFrame {
	WindowBoundary start;
	WindowBoundary end;
	WindowExcludeMode exclude_clause = WindowExcludeMode::NO_OTHER;

	unique_ptr<ParsedExpression> start_expr;
	unique_ptr<ParsedExpression> end_expr;
};

struct WindowBoundaryExpression {
	WindowBoundary boundary;
	unique_ptr<ParsedExpression> expr;
};

} // namespace duckdb
