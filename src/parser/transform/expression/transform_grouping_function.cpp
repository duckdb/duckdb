#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformGroupingFunction(duckdb_libpgquery::PGGroupingFunc &grouping) {
	auto op = make_uniq<OperatorExpression>(ExpressionType::GROUPING_FUNCTION);
	for (auto node = grouping.args->head; node; node = node->next) {
		auto n = PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
		op->children.push_back(TransformExpression(n));
	}
	op->query_location = grouping.location;
	return std::move(op);
}

} // namespace duckdb
