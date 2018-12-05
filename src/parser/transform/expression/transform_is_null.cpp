#include "parser/expression/operator_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<Expression> Transformer::TransformNullTest(NullTest *root) {
	if (!root) {
		return nullptr;
	}
	auto arg = TransformExpression(reinterpret_cast<Node *>(root->arg));
	if (root->argisrow) {
		throw NotImplementedException("IS NULL argisrow");
	}
	ExpressionType expr_type =
	    (root->nulltesttype == IS_NULL) ? ExpressionType::OPERATOR_IS_NULL : ExpressionType::OPERATOR_IS_NOT_NULL;

	return unique_ptr<Expression>(new OperatorExpression(expr_type, TypeId::BOOLEAN, move(arg)));
}
