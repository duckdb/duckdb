#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

static string ExtractColumnFromLambda(ParsedExpression &expr) {
	if (expr.type != ExpressionType::COLUMN_REF) {
		throw ParserException("Lambda parameter must be a column name");
	}
	auto &colref = (ColumnRefExpression &)expr;
	if (colref.IsQualified()) {
		throw ParserException("Lambda parameter must be an unqualified name (e.g. 'x', not 'a.x')");
	}
	return colref.column_names[0];
}

unique_ptr<ParsedExpression> Transformer::TransformLambda(duckdb_libpgquery::PGLambdaFunction *node) {
	if (!node->parameters) {
		throw ParserException("Lambda function must have parameters");
	}
	vector<unique_ptr<ParsedExpression>> parameter_expressions;
	TransformExpressionList(*node->parameters, parameter_expressions);
	vector<string> parameters;
	parameters.reserve(parameter_expressions.size());
	for (auto &expr : parameter_expressions) {
		parameters.push_back(ExtractColumnFromLambda(*expr));
	}

	auto lambda_function = TransformExpression(node->function);
	return make_unique<LambdaExpression>(move(parameters), move(lambda_function));
}

} // namespace duckdb
