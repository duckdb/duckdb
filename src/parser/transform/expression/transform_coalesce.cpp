#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

// COALESCE(a,b,c) returns the first argument that is NOT NULL, so
// rewrite into CASE(a IS NOT NULL, a, CASE(b IS NOT NULL, b, c))
unique_ptr<ParsedExpression> Transformer::TransformCoalesce(PGAExpr *root) {
	if (!root) {
		return nullptr;
	}
	auto coalesce_args = reinterpret_cast<PGList *>(root->lexpr);

	auto exp_root = make_unique<CaseExpression>();
	auto cur_root = exp_root.get();
	for (auto cell = coalesce_args->head; cell && cell->next; cell = cell->next) {
		// get the value of the COALESCE
		auto value_expr = TransformExpression(reinterpret_cast<PGNode *>(cell->data.ptr_value));
		// perform an IS NOT NULL comparison with the value here
		cur_root->check = make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, value_expr->Copy());
		// if IS NOT NULL, we output the value
		cur_root->result_if_true = move(value_expr);
		if (cell->next->next == nullptr) {
			// if there is no next in the chain, the COALESCE ends there
			cur_root->result_if_false = TransformExpression(reinterpret_cast<PGNode *>(cell->next->data.ptr_value));
		} else {
			// more COALESCE parameters remain, create a nested CASE statement
			auto next_case = make_unique<CaseExpression>();
			auto case_ptr = next_case.get();
			cur_root->result_if_false = move(next_case);
			cur_root = case_ptr;
		}
	}
	return move(exp_root);
}
