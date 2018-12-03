
#include "parser/expression/case_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<Expression> Transformer::TransformCase(CaseExpr *root) {
	if (!root) {
		return nullptr;
	}
	// CASE expression WHEN value THEN result [WHEN ...] ELSE result uses this,
	// but we rewrite to CASE WHEN expression = value THEN result ... to only
	// have to handle one case downstream.

	unique_ptr<Expression> def_res;
	if (root->defresult) {
		def_res =
		    TransformExpression(reinterpret_cast<Node *>(root->defresult));
	} else {
		def_res = unique_ptr<Expression>(new ConstantExpression(Value()));
	}
	// def_res will be the else part of the innermost case expression

	// CASE WHEN e1 THEN r1 WHEN w2 THEN r2 ELSE r3 is rewritten to
	// CASE WHEN e1 THEN r1 ELSE CASE WHEN e2 THEN r2 ELSE r3

	auto exp_root = unique_ptr<Expression>(new CaseExpression());
	Expression *cur_root = exp_root.get();
	Expression *next_root = nullptr;

	for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
		CaseWhen *w = reinterpret_cast<CaseWhen *>(cell->data.ptr_value);

		auto test_raw = TransformExpression(reinterpret_cast<Node *>(w->expr));
		unique_ptr<Expression> test;
		// TODO: how do we copy those things?
		auto arg = TransformExpression(reinterpret_cast<Node *>(root->arg));

		if (arg) {
			test = unique_ptr<Expression>(new ComparisonExpression(
			    ExpressionType::COMPARE_EQUAL, move(arg), move(test_raw)));
		} else {
			test = move(test_raw);
		}

		auto res_true =
		    TransformExpression(reinterpret_cast<Node *>(w->result));

		unique_ptr<Expression> res_false;
		if (cell->next == nullptr) {
			res_false = move(def_res);
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
