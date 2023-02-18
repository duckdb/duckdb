#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformPivot(duckdb_libpgquery::PGPivotExpr *root) {
	auto result = make_unique<PivotRef>();
	result->source = TransformTableRefNode(root->source);
	if (root->aggr) {
		result->aggregate = TransformExpression(root->aggr);
	}
	if (root->unpivot) {
		result->unpivot_name = root->unpivot;
	}
	for (auto node = root->pivots->head; node != nullptr; node = node->next) {
		auto pivot = (duckdb_libpgquery::PGPivot *)node->data.ptr_value;

		PivotColumn col;
		col.name = pivot->pivot_column;
		if (pivot->pivot_value) {
			for (auto node = pivot->pivot_value->head; node != nullptr; node = node->next) {
				auto n = (duckdb_libpgquery::PGNode *)node->data.ptr_value;
				auto expr = TransformExpression(n);
				Value val;
				if (expr->type == ExpressionType::COLUMN_REF) {
					auto &colref = (ColumnRefExpression &)*expr;
					if (colref.IsQualified()) {
						throw ParserException("PIVOT IN list cannot contain qualified column references");
					}
					val = Value(colref.GetColumnName());
				} else if (expr->type == ExpressionType::VALUE_CONSTANT) {
					auto &constant_expr = (ConstantExpression &)*expr;
					val = std::move(constant_expr.value);
				} else {
					throw ParserException("PIVOT IN list cannot contain expressions");
				}
				col.values.emplace_back(std::move(val));
			}
		} else {
			col.pivot_enum = pivot->pivot_enum;
		}
		result->pivots.push_back(std::move(col));
	}
	if (root->groups) {
		result->groups = TransformStringList(root->groups);
	}
	result->alias = TransformAlias(root->alias, result->column_name_alias);
	return std::move(result);
}

} // namespace duckdb
