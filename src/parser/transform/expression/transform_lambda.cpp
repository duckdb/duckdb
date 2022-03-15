#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformLambda(duckdb_libpgquery::PGLambdaFunction *node) {
	if (!node->lhs) {
		throw ParserException("Lambda function must have parameters");
	}
	auto lhs = TransformExpression(node->lhs);
	auto rhs = TransformExpression(node->rhs);
	return make_unique<LambdaExpression>(move(lhs), move(rhs));
}

} // namespace duckdb
