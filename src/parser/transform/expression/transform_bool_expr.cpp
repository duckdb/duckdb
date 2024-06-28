#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformBoolExpr(duckdb_libpgquery::PGBoolExpr &root) {
	unique_ptr<ParsedExpression> result;
	for (auto node = root.args->head; node != nullptr; node = node->next) {
		auto next = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value));

		switch (root.boolop) {
		case duckdb_libpgquery::PG_AND_EXPR: {
			if (!result) {
				result = std::move(next);
			} else {
				result = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(result),
				                                          std::move(next));
			}
			break;
		}
		case duckdb_libpgquery::PG_OR_EXPR: {
			if (!result) {
				result = std::move(next);
			} else {
				result = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(result),
				                                          std::move(next));
			}
			break;
		}
		case duckdb_libpgquery::PG_NOT_EXPR: {
			if (next->type == ExpressionType::COMPARE_IN) {
				// convert COMPARE_IN to COMPARE_NOT_IN
				next->type = ExpressionType::COMPARE_NOT_IN;
				result = std::move(next);
			} else if (next->type >= ExpressionType::COMPARE_EQUAL &&
			           next->type <= ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				// NOT on a comparison: we can negate the comparison
				// e.g. NOT(x > y) is equivalent to x <= y
				next->type = NegateComparisonExpression(next->type);
				result = std::move(next);
			} else {
				result = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(next));
			}
			break;
		}
		}
	}
	SetQueryLocation(*result, root.location);
	return result;
}

} // namespace duckdb
