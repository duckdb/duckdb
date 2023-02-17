#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformPivot(duckdb_libpgquery::PGPivotExpr *root) {
	auto result = make_unique<PivotRef>();
	result->source = TransformTableRefNode(root->source);
	result->aggregate = TransformExpression(root->aggr);
	for (auto node = root->pivots->head; node != nullptr; node = node->next) {
		auto pivot = (duckdb_libpgquery::PGPivot *)node->data.ptr_value;

		PivotColumn col;
		col.name = pivot->pivot_column;
		for (auto node = pivot->pivot_value->head; node != nullptr; node = node->next) {
			auto n = (duckdb_libpgquery::PGNode *)node->data.ptr_value;
			if (n->type != duckdb_libpgquery::T_PGAConst) {
				throw ParserException("PIVOT IN list can only contain constant values");
			}
			auto constant = TransformConstant((duckdb_libpgquery::PGAConst *)n);
			auto &constant_expr = (ConstantExpression &)*constant;
			col.values.emplace_back(std::move(constant_expr.value));
		}
		result->pivots.push_back(std::move(col));
	}
	result->alias = TransformAlias(root->alias, result->column_name_alias);
	return std::move(result);
}

} // namespace duckdb
