#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformBooleanTest(duckdb_libpgquery::PGBooleanTest *node) {
	auto argument = TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(node->arg));

	auto expr_true = make_unique<ConstantExpression>(Value::BOOLEAN(true));
	auto expr_false = make_unique<ConstantExpression>(Value::BOOLEAN(false));
	// we cast the argument to bool to remove ambiguity wrt function binding on the comparision
	auto cast_argument = make_unique<CastExpression>(LogicalType::BOOLEAN, argument->Copy());

	switch (node->booltesttype) {
	case duckdb_libpgquery::PGBoolTestType::PG_IS_TRUE:
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(cast_argument),
		                                         std::move(expr_true));
	case duckdb_libpgquery::PGBoolTestType::IS_NOT_TRUE:
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_DISTINCT_FROM, std::move(cast_argument),
		                                         std::move(expr_true));
	case duckdb_libpgquery::PGBoolTestType::IS_FALSE:
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(cast_argument),
		                                         std::move(expr_false));
	case duckdb_libpgquery::PGBoolTestType::IS_NOT_FALSE:
		return make_unique<ComparisonExpression>(ExpressionType::COMPARE_DISTINCT_FROM, std::move(cast_argument),
		                                         std::move(expr_false));
	case duckdb_libpgquery::PGBoolTestType::IS_UNKNOWN: // IS NULL
		return make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, std::move(argument));
	case duckdb_libpgquery::PGBoolTestType::IS_NOT_UNKNOWN: // IS NOT NULL
		return make_unique<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(argument));
	default:
		throw NotImplementedException("Unknown boolean test type %d", node->booltesttype);
	}
}

} // namespace duckdb
