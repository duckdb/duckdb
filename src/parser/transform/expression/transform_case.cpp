#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformCase(duckdb_libpgquery::PGCaseExpr *root) {
	D_ASSERT(root);

	auto case_node = make_unique<CaseExpression>();
	for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
		CaseCheck case_check;

		auto w = reinterpret_cast<duckdb_libpgquery::PGCaseWhen *>(cell->data.ptr_value);
		auto test_raw = TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(w->expr));
		unique_ptr<ParsedExpression> test;
		auto arg = TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(root->arg));
		if (arg) {
			case_check.when_expr =
			    make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(arg), std::move(test_raw));
		} else {
			case_check.when_expr = std::move(test_raw);
		}
		case_check.then_expr = TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(w->result));
		case_node->case_checks.push_back(std::move(case_check));
	}

	if (root->defresult) {
		case_node->else_expr = TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(root->defresult));
	} else {
		case_node->else_expr = make_unique<ConstantExpression>(Value(LogicalType::SQLNULL));
	}
	return std::move(case_node);
}

} // namespace duckdb
