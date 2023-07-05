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

const ParsedExpression &DuckDBPyExpression::GetExpression() const {
	return *expression;
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Add(const DuckDBPyExpression &other) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(GetExpression().Copy());
	children.push_back(other.GetExpression().Copy());
	return DuckDBPyExpression::FunctionExpression("+", std::move(children));
}

// Static creation methods

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::FunctionExpression(const string &function_name,
                                                                      vector<unique_ptr<ParsedExpression>> children) {
	auto function_expression = make_uniq<duckdb::FunctionExpression>(function_name, std::move(children));
	return make_shared<DuckDBPyExpression>(std::move(function_expression));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::BinaryFunctionExpression(const string &function_name,
                                                                            shared_ptr<DuckDBPyExpression> arg_one,
                                                                            shared_ptr<DuckDBPyExpression> arg_two) {
	vector<unique_ptr<ParsedExpression>> children;

	children.push_back(arg_one->GetExpression().Copy());
	children.push_back(arg_two->GetExpression().Copy());
	return FunctionExpression(function_name, std::move(children));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::ColumnExpression(const string &column_name) {
	return make_shared<DuckDBPyExpression>(make_uniq<duckdb::ColumnRefExpression>(column_name));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::ConstantExpression(const PythonValue &value) {
	auto val = TransformPythonValue(value);
	return make_shared<DuckDBPyExpression>(make_uniq<duckdb::ConstantExpression>(std::move(val)));
}

} // namespace duckdb
