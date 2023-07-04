#include "duckdb_python/expression/pyexpression.hpp"

namespace duckdb {

DuckDBPyExpression::DuckDBPyExpression(unique_ptr<ParsedExpression> expr_p) : expression(std::move(expr_p)) {
	if (!expression) {
		throw InternalException("DuckDBPyExpression created without an expression");
	}
}

string DuckDBPyExpression::Type() const {
	return ExpressionTypeToString(expression->type);
}

string DuckDBPyExpression::ToString() const {
	return expression->ToString();
}

void DuckDBPyExpression::Print() const {
	Printer::Print(expression->ToString());
}

} // namespace duckdb
