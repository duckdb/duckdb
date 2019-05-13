#include "common/exception.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformNullTest(NullTest *root) {
	assert(root);
	auto arg = TransformExpression(reinterpret_cast<Node *>(root->arg));
	if (root->argisrow) {
		throw NotImplementedException("IS NULL argisrow");
	}
	ExpressionType expr_type =
	    (root->nulltesttype == IS_NULL) ? ExpressionType::OPERATOR_IS_NULL : ExpressionType::OPERATOR_IS_NOT_NULL;

	return unique_ptr<ParsedExpression>(new OperatorExpression(expr_type, move(arg)));
}
