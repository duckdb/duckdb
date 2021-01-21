#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

using namespace duckdb_libpgquery;

unique_ptr<ParsedExpression> Transformer::TransformLambda(unique_ptr<ParsedExpression> left, unique_ptr<ParsedExpression> right) {
	if (left->type != ExpressionType::COLUMN_REF) {
		throw ParserException("Left side of a lambda must be a single column");
	}
	auto &colref = (ColumnRefExpression &) *left;
	if (!colref.table_name.empty()) {
		throw ParserException("Lambda capture must be an unqualified name (e.g. 'x', not 'a.x')");
	}
	return make_unique<LambdaExpression>(colref.column_name, move(right));
}

} // namespace duckdb
