#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformGroupingFunction(duckdb_libpgquery::PGGroupingFunc *n) {
	auto op = make_unique<OperatorExpression>(ExpressionType::GROUPING_FUNCTION);
	for (auto node = n->args->head; node; node = node->next) {
		auto n = (duckdb_libpgquery::PGNode *)node->data.ptr_value;
		op->children.push_back(TransformExpression(n));
	}
	op->query_location = n->location;
	return std::move(op);
}

} // namespace duckdb
