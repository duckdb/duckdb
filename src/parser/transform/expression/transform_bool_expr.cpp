#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformBoolExpr(postgres::BoolExpr *root) {
	unique_ptr<ParsedExpression> result;
	for (auto node = root->args->head; node != nullptr; node = node->next) {
		auto next = TransformExpression(reinterpret_cast<postgres::Node *>(node->data.ptr_value));

		switch (root->boolop) {
		case postgres::AND_EXPR: {
			if (!result) {
				result = move(next);
			} else {
				result = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, move(result), move(next));
			}
			break;
		}
		case postgres::OR_EXPR: {
			if (!result) {
				result = move(next);
			} else {
				result = make_unique<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, move(result), move(next));
			}
			break;
		}
		case postgres::NOT_EXPR: {
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
