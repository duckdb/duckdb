#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformSubquery(postgres::SubLink *root) {
	if (!root) {
		return nullptr;
	}
	auto subquery_expr = make_unique<SubqueryExpression>();
	subquery_expr->subquery = TransformSelectNode((postgres::SelectStmt *)root->subselect);
	if (!subquery_expr->subquery) {
		return nullptr;
	}
	auto &select_list = subquery_expr->subquery->GetSelectList();
	assert(select_list.size() > 0);

	switch (root->subLinkType) {
	case postgres::EXISTS_SUBLINK: {
		subquery_expr->subquery_type = SubqueryType::EXISTS;
		break;
	}
	case postgres::ANY_SUBLINK:
	case postgres::ALL_SUBLINK: {
		// comparison with ANY() or ALL()
		subquery_expr->subquery_type = SubqueryType::ANY;
		subquery_expr->child = TransformExpression(root->testexpr);
		// get the operator name
		if (!root->operName) {
			// simple IN
			subquery_expr->comparison_type = ExpressionType::COMPARE_EQUAL;
		} else {
			auto operator_name =
			    string((reinterpret_cast<postgres::Value *>(root->operName->head->data.ptr_value))->val.str);
			subquery_expr->comparison_type = OperatorToExpressionType(operator_name);
		}
		assert(subquery_expr->comparison_type == ExpressionType::COMPARE_EQUAL ||
		       subquery_expr->comparison_type == ExpressionType::COMPARE_NOTEQUAL ||
		       subquery_expr->comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
		       subquery_expr->comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
		       subquery_expr->comparison_type == ExpressionType::COMPARE_LESSTHAN ||
		       subquery_expr->comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO);
		if (root->subLinkType == postgres::ALL_SUBLINK) {
			// ALL sublink is equivalent to NOT(ANY) with inverted comparison
			// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
			// first invert the comparison type
			subquery_expr->comparison_type = NegateComparisionExpression(subquery_expr->comparison_type);
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, move(subquery_expr));
		}
		break;
	}
	case postgres::EXPR_SUBLINK: {
		// return a single scalar value from the subquery
		// no child expression to compare to
		subquery_expr->subquery_type = SubqueryType::SCALAR;
		break;
	}
	default:
		throw NotImplementedException("Subquery of type %d not implemented\n", (int)root->subLinkType);
	}
	return move(subquery_expr);
}
