#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformLambda(duckdb_libpgquery::PGLambdaFunction *node) {

	D_ASSERT(node->lhs);
	D_ASSERT(node->rhs);

	vector<unique_ptr<ParsedExpression>> lhs;
	TransformExpressionList(*node->lhs, lhs);

	auto rhs = TransformExpression(node->rhs);
	return make_unique<LambdaExpression>(move(lhs), move(rhs));
}

} // namespace duckdb
