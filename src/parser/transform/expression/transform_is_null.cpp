#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformNullTest(duckdb_libpgquery::PGNullTest &root) {
	auto arg = TransformExpression(PGPointerCast<duckdb_libpgquery::PGNode>(root.arg));
	if (root.argisrow) {
		throw NotImplementedException("IS NULL argisrow");
	}
	ExpressionType expr_type = (root.nulltesttype == duckdb_libpgquery::PG_IS_NULL)
	                               ? ExpressionType::OPERATOR_IS_NULL
	                               : ExpressionType::OPERATOR_IS_NOT_NULL;

	auto result = make_uniq<OperatorExpression>(expr_type, std::move(arg));
	SetQueryLocation(*result, root.location);
	return std::move(result);
}

} // namespace duckdb
