#include "duckdb/common/exception.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformPositionalReference(duckdb_libpgquery::PGPositionalReference &node) {
	if (node.position <= 0) {
		throw ParserException("Positional reference node needs to be >= 1");
	}
	auto result = make_uniq<PositionalReferenceExpression>(NumericCast<idx_t>(node.position));
	SetQueryLocation(*result, node.location);
	return std::move(result);
}

} // namespace duckdb
