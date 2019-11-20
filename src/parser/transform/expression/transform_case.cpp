#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformCase(PGCaseExpr *root) {
	if (!root) {
		return nullptr;
	}
	// CASE expression WHEN value THEN result [WHEN ...] ELSE result uses this,
	// but we rewrite to CASE WHEN expression = value THEN result ... to only
	// have to handle one case downstream.

	unique_ptr<ParsedExpression> def_res;
	if (root->defresult) {
		def_res = TransformExpression(reinterpret_cast<PGNode *>(root->defresult));
	} else {
		def_res = make_unique<ConstantExpression>(SQLType::SQLNULL, Value());
	}
	// def_res will be the else part of the innermost case expression

	// CASE WHEN e1 THEN r1 WHEN w2 THEN r2 ELSE r3 is rewritten to
	// CASE WHEN e1 THEN r1 ELSE CASE WHEN e2 THEN r2 ELSE r3

	auto exp_root = make_unique<CaseExpression>();
	auto cur_root = exp_root.get();
	for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
		auto w = reinterpret_cast<PGCaseWhen *>(cell->data.ptr_value);
		auto test_raw = TransformExpression(reinterpret_cast<PGNode *>(w->expr));
		unique_ptr<ParsedExpression> test;
		auto arg = TransformExpression(reinterpret_cast<PGNode *>(root->arg));
		if (arg) {
			test = make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(arg), move(test_raw));
		} else {
			test = move(test_raw);
		}

		cur_root->check = move(test);
		cur_root->result_if_true = TransformExpression(reinterpret_cast<PGNode *>(w->result));
		if (cell->next == nullptr) {
			// finished all cases
			// res_false is the default result
			cur_root->result_if_false = move(def_res);
		} else {
			// more cases remain, create a case statement within the FALSE branch
			auto next_case = make_unique<CaseExpression>();
			auto case_ptr = next_case.get();
			cur_root->result_if_false = move(next_case);
			cur_root = case_ptr;
		}
	}

	return move(exp_root);
}
