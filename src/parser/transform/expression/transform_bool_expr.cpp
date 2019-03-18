#include "parser/expression/conjunction_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformBoolExpr(BoolExpr *root) {
	unique_ptr<ParsedExpression> result;
	for (auto node = root->args->head; node != nullptr; node = node->next) {
		auto next = TransformExpression(reinterpret_cast<Node *>(node->data.ptr_value));

		switch (root->boolop) {
		case AND_EXPR: {
			if (!result) {
				result = move(next);
			} else {
				result = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(result), move(next));
			}
			break;
		}
		case OR_EXPR: {
			if (!result) {
				result = move(next);
			} else {
				result = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, move(result), move(next));
			}
			break;
		}
		case NOT_EXPR: {
			if (next->type == ExpressionType::COMPARE_IN) {
				// convert COMPARE_IN to COMPARE_NOT_IN
				next->type = ExpressionType::COMPARE_NOT_IN;
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
