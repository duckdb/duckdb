#include "duckdb_python/expression/pyexpression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

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

// Binary operators

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Add(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("+", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Subtract(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("-", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Multiply(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("*", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Division(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("/", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::FloorDivision(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("//", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Modulo(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("%", *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Power(const DuckDBPyExpression &other) {
	return DuckDBPyExpression::BinaryOperator("**", *this, other);
}

// Comparison expressions

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Equality(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_EQUAL, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Inequality(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_NOTEQUAL, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::GreaterThan(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_GREATERTHAN, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::GreaterThanOrEqual(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_GREATERTHANOREQUALTO, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::LessThan(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_LESSTHAN, *this, other);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::LessThanOrEqual(const DuckDBPyExpression &other) {
	return ComparisonExpression(ExpressionType::COMPARE_LESSTHANOREQUALTO, *this, other);
}

// Unary operators

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::Negate() {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(GetExpression().Copy());
	return DuckDBPyExpression::FunctionExpression("-", std::move(children), true);
}

// Static creation methods

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

// Private methods

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::BinaryOperator(const string &function_name,
                                                                  const DuckDBPyExpression &arg_one,
                                                                  const DuckDBPyExpression &arg_two) {
	vector<unique_ptr<ParsedExpression>> children;

	children.push_back(arg_one.GetExpression().Copy());
	children.push_back(arg_two.GetExpression().Copy());
	return FunctionExpression(function_name, std::move(children), true);
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::FunctionExpression(const string &function_name,
                                                                      vector<unique_ptr<ParsedExpression>> children,
                                                                      bool is_operator) {
	auto function_expression =
	    make_uniq<duckdb::FunctionExpression>(function_name, std::move(children), nullptr, nullptr, false, is_operator);
	return make_shared<DuckDBPyExpression>(std::move(function_expression));
}

shared_ptr<DuckDBPyExpression> DuckDBPyExpression::ComparisonExpression(ExpressionType type,
                                                                        const DuckDBPyExpression &left_p,
                                                                        const DuckDBPyExpression &right_p) {
	auto left = left_p.GetExpression().Copy();
	auto right = right_p.GetExpression().Copy();
	return make_shared<DuckDBPyExpression>(
	    make_uniq<duckdb::ComparisonExpression>(type, std::move(left), std::move(right)));
}

} // namespace duckdb
