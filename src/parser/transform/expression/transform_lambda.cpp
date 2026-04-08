#include <memory>
#include <utility>

#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformLambda(duckdb_libpgquery::PGLambdaFunction &node) {
	D_ASSERT(node.lhs);
	D_ASSERT(node.rhs);

	auto parameters = TransformStringList(node.lhs);
	auto rhs = TransformExpression(node.rhs);
	D_ASSERT(!parameters.empty());
	D_ASSERT(rhs);
	auto result = make_uniq<LambdaExpression>(std::move(parameters), std::move(rhs));
	SetQueryLocation(*result, node.location);
	return std::move(result);
}

unique_ptr<ParsedExpression> Transformer::TransformSingleArrow(duckdb_libpgquery::PGSingleArrowFunction &node) {
	D_ASSERT(node.lhs);
	D_ASSERT(node.rhs);

	auto lhs = TransformExpression(node.lhs);
	auto rhs = TransformExpression(node.rhs);
	D_ASSERT(lhs);
	D_ASSERT(rhs);
	auto result = make_uniq<LambdaExpression>(std::move(lhs), std::move(rhs));
	SetQueryLocation(*result, node.location);
	return std::move(result);
}

} // namespace duckdb
