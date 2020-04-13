#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformBoolExpr(PGBoolExpr *root) {
	unique_ptr<ParsedExpression> result;
	for (auto node = root->args->head; node != nullptr; node = node->next) {
		auto next = TransformExpression(reinterpret_cast<PGNode *>(node->data.ptr_value));

		switch (root->boolop) {
		case PG_AND_EXPR: {
			if (!result) {
				result = move(next);
			} else {
				result = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(result), move(next));
			}
			break;
		}
		case PG_OR_EXPR: {
			if (!result) {
				result = move(next);
			} else {
				result = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, move(result), move(next));
			}
			break;
		}
		case PG_NOT_EXPR: {
			if (next->type == ExpressionType::COMPARE_IN) {
				// convert COMPARE_IN to COMPARE_NOT_IN
				next->type = ExpressionType::COMPARE_NOT_IN;
				result = move(next);
			} else if (next->type >= ExpressionType::COMPARE_EQUAL &&
			           next->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				// NOT on a comparison: we can negate the comparison
				// e.g. NOT(x > y) is equivalent to x <= y
				next->type = NegateComparisionExpression(next->type);
				result = move(next);
			} else {
				result = make_unique<OperatorExpression>(ExpressionType::OPERATOR_NOT, move(next));
			}
			break;
		}
		}
	}
	return result;
}
