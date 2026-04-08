#include <memory>
#include <utility>

#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "nodes/nodes.hpp"
#include "nodes/pg_list.hpp"
#include "nodes/primnodes.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformGroupingFunction(duckdb_libpgquery::PGGroupingFunc &grouping) {
	auto op = make_uniq<OperatorExpression>(ExpressionType::GROUPING_FUNCTION);
	for (auto node = grouping.args->head; node; node = node->next) {
		auto n = PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
		op->children.push_back(TransformExpression(n));
	}
	SetQueryLocation(*op, grouping.location);
	return std::move(op);
}

} // namespace duckdb
