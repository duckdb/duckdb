#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformLambda(duckdb_libpgquery::PGLambdaFunction *node) {

	D_ASSERT(node->lhs);
	D_ASSERT(node->rhs);

	auto lhs = TransformExpression(node->lhs);
	auto rhs = TransformExpression(node->rhs);
	D_ASSERT(lhs);
	D_ASSERT(rhs);
	return make_unique<LambdaExpression>(move(lhs), move(rhs));
}

} // namespace duckdb
