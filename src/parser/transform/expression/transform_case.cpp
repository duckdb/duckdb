#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

using namespace duckdb_libpgquery;

unique_ptr<ParsedExpression> Transformer::TransformCase(PGCaseExpr *root) {
	if (!root) {
		return nullptr;
	}
	// CASE expression WHEN value THEN result [WHEN ...] ELSE result uses this,
	// but we rewrite to CASE WHEN expression = value THEN result ... to only
	// have to handle one case downstream.
	auto case_node = make_unique<CaseExpression>();
	for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
		CaseCheck case_check;

		auto w = reinterpret_cast<PGCaseWhen *>(cell->data.ptr_value);
		auto test_raw = TransformExpression(reinterpret_cast<PGNode *>(w->expr));
		unique_ptr<ParsedExpression> test;
		auto arg = TransformExpression(reinterpret_cast<PGNode *>(root->arg));
		if (arg) {
			case_check.when_expr = make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(arg), move(test_raw));
		} else {
			case_check.when_expr = move(test_raw);
		}
		case_check.then_expr = TransformExpression(reinterpret_cast<PGNode *>(w->result));
		case_node->case_checks.push_back(move(case_check));
	}

	if (root->defresult) {
		case_node->else_expr = TransformExpression(reinterpret_cast<PGNode *>(root->defresult));
	} else {
		case_node->else_expr = make_unique<ConstantExpression>(Value(LogicalType::SQLNULL));
	}
	return move(case_node);
}

} // namespace duckdb
