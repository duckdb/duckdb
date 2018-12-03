
#include "parser/expression/case_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

// COALESCE(a,b,c) returns the first argument that is NOT NULL, so
// rewrite into CASE(a IS NOT NULL, a, CASE(b IS NOT NULL, b, c))
unique_ptr<Expression> Transformer::TransformCoalesce(A_Expr *root) {
	if (!root) {
		return nullptr;
	}
	auto coalesce_args = reinterpret_cast<List *>(root->lexpr);
	// TODO: this is somewhat duplicated from the CASE rewrite below, perhaps
	// they can be merged
	auto exp_root = unique_ptr<Expression>(new CaseExpression());
	Expression *cur_root = exp_root.get();
	Expression *next_root = nullptr;

	for (auto cell = coalesce_args->head; cell && cell->next;
	     cell = cell->next) {
		// we need this twice
		auto value_expr =
		    TransformExpression(reinterpret_cast<Node *>(cell->data.ptr_value));
		auto res_true =
		    TransformExpression(reinterpret_cast<Node *>(cell->data.ptr_value));

		auto test = unique_ptr<Expression>(
		    new OperatorExpression(ExpressionType::OPERATOR_IS_NOT_NULL,
		                           TypeId::BOOLEAN, move(value_expr)));

		// the last argument does not need its own CASE because if we get there
		// we might as well return it directly
		unique_ptr<Expression> res_false;
		if (cell->next->next == nullptr) {
			res_false = TransformExpression(
			    reinterpret_cast<Node *>(cell->next->data.ptr_value));
		} else {
			res_false = unique_ptr<Expression>(new CaseExpression());
			next_root = res_false.get();
		}
		cur_root->AddChild(move(test));
		cur_root->AddChild(move(res_true));
		cur_root->AddChild(move(res_false));
		cur_root = next_root;
	}
	return exp_root;
}
