#include "parser/expression/operator_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
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
		subquery_expr->return_type = TypeId::BOOLEAN;
		break;
	}
	case ANY_SUBLINK: 
	case ALL_SUBLINK: {
		// comparison with ANY() or ALL()
		subquery_expr->subquery_type = SubqueryType::ANY;
		subquery_expr->child = TransformExpression(root->testexpr);
		subquery_expr->return_type = TypeId::BOOLEAN;
		// get the operator name
		if (!root->operName) {
			// simple IN 
			subquery_expr->comparison_type = ExpressionType::COMPARE_EQUAL;
		} else {
			auto operator_name = string((reinterpret_cast<value *>(root->operName->head->data.ptr_value))->val.str);
			subquery_expr->comparison_type = OperatorToExpressionType(operator_name);
		}
		assert(subquery_expr->comparison_type == ExpressionType::COMPARE_EQUAL ||
			   subquery_expr->comparison_type == ExpressionType::COMPARE_NOTEQUAL ||
			   subquery_expr->comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
			   subquery_expr->comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
			   subquery_expr->comparison_type == ExpressionType::COMPARE_LESSTHAN ||
			   subquery_expr->comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO);
		if (root->subLinkType == ALL_SUBLINK) {
			// ALL sublink is equivalent to NOT(ANY) with inverted comparison
			// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
			// first invert the comparison type
			subquery_expr->comparison_type = ComparisonExpression::NegateComparisionExpression(subquery_expr->comparison_type);
			return make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, TypeId::BOOLEAN, move(subquery_expr), nullptr);
		}
		break;
	}
	case EXPR_SUBLINK: {
		// return a single scalar value from the subquery
		// no child expression to compare to
		subquery_expr->subquery_type = SubqueryType::SCALAR;
		break;
	}
	default: { throw NotImplementedException("Subquery of type %d not implemented\n", (int)root->subLinkType); }
	}
	return subquery_expr;
}
