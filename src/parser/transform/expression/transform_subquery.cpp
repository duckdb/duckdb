#include "parser/expression/operator_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<Expression> Transformer::TransformSubquery(SubLink *root) {
	if (!root) {
		return nullptr;
	}
	auto subquery_expr = make_unique<SubqueryExpression>();
	subquery_expr->subquery = TransformSelectNode((SelectStmt *)root->subselect);
	if (!subquery_expr->subquery) {
		return nullptr;
	}

	switch (root->subLinkType) {
	case EXISTS_SUBLINK: {
		subquery_expr->subquery_type = SubqueryType::EXISTS;
		return make_unique<OperatorExpression>(ExpressionType::OPERATOR_EXISTS, TypeId::BOOLEAN, move(subquery_expr));
	}
	case ANY_SUBLINK: {
		subquery_expr->subquery_type = SubqueryType::IN;
		return make_unique<OperatorExpression>(ExpressionType::COMPARE_IN, TypeId::BOOLEAN,
		                                       TransformExpression(root->testexpr), move(subquery_expr));
	}
	case EXPR_SUBLINK: {
		return subquery_expr;
	}
	default: { throw NotImplementedException("Subquery of type %d not implemented\n", (int)root->subLinkType); }
	}
}
