#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformNullTest(postgres::NullTest *root) {
	assert(root);
	auto arg = TransformExpression(reinterpret_cast<postgres::Node *>(root->arg));
	if (root->argisrow) {
		throw NotImplementedException("IS NULL argisrow");
	}
	ExpressionType expr_type = (root->nulltesttype == postgres::IS_NULL) ? ExpressionType::OPERATOR_IS_NULL
	                                                                     : ExpressionType::OPERATOR_IS_NOT_NULL;

	return unique_ptr<ParsedExpression>(new OperatorExpression(expr_type, move(arg)));
}
