#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformCase(duckdb_libpgquery::PGCaseExpr &root) {
	auto case_node = make_uniq<CaseExpression>();
	auto root_arg = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(root.arg));
	for (auto cell = root.args->head; cell != nullptr; cell = cell->next) {
		CaseCheck case_check;

		auto w = PGPointerCast<duckdb_libpgquery::PGCaseWhen>(cell->data.ptr_value);
		auto test_raw = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(w->expr));
		unique_ptr<ParsedExpression> test;
		if (root_arg) {
			case_check.when_expr =
			    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, root_arg->Copy(), std::move(test_raw));
		} else {
			case_check.when_expr = std::move(test_raw);
		}
		case_check.then_expr = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(w->result));
		case_node->case_checks.push_back(std::move(case_check));
	}

	if (root.defresult) {
		case_node->else_expr = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(root.defresult));
	} else {
		case_node->else_expr = make_uniq<ConstantExpression>(Value(LogicalType::SQLNULL));
	}
	return std::move(case_node);
}

} // namespace duckdb
