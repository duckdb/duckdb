#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

PivotColumn Transformer::TransformPivotColumn(duckdb_libpgquery::PGPivot &pivot, bool is_pivot) {
	PivotColumn col;
	if (pivot.pivot_columns) {
		TransformExpressionList(*pivot.pivot_columns, col.pivot_expressions);
		for (auto &expr : col.pivot_expressions) {
			if (expr->IsScalar()) {
				throw ParserException(expr->GetQueryLocation(), "Cannot pivot on constant value \"%s\"",
				                      expr->ToString());
			}
			if (expr->HasSubquery()) {
				throw ParserException(expr->GetQueryLocation(), "Cannot pivot on subquery \"%s\"", expr->ToString());
			}
		}
	} else if (pivot.unpivot_columns) {
		col.unpivot_names = TransformStringList(pivot.unpivot_columns);
	} else {
		throw InternalException("Either pivot_columns or unpivot_columns must be defined");
	}
	if (pivot.pivot_value) {
		for (auto node = pivot.pivot_value->head; node != nullptr; node = node->next) {
			auto n = PGPointerCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
			auto expr = TransformExpression(n);
			PivotColumnEntry entry;
			entry.alias = expr->GetAlias();
			entry.expr = std::move(expr);
			col.entries.push_back(std::move(entry));
		}
	}
	if (pivot.subquery) {
		col.subquery = TransformSelectNode(*pivot.subquery);
	}
	if (pivot.pivot_enum) {
		col.pivot_enum = pivot.pivot_enum;
	}
	return col;
}

vector<PivotColumn> Transformer::TransformPivotList(duckdb_libpgquery::PGList &list, bool is_pivot) {
	vector<PivotColumn> result;
	for (auto node = list.head; node != nullptr; node = node->next) {
		auto pivot = PGPointerCast<duckdb_libpgquery::PGPivot>(node->data.ptr_value);
		result.push_back(TransformPivotColumn(*pivot, is_pivot));
	}
	return result;
}

unique_ptr<TableRef> Transformer::TransformPivot(duckdb_libpgquery::PGPivotExpr &root) {
	auto result = make_uniq<PivotRef>();
	result->source = TransformTableRefNode(*root.source);
	if (root.aggrs) {
		TransformExpressionList(*root.aggrs, result->aggregates);
	}
	if (root.unpivots) {
		result->unpivot_names = TransformStringList(root.unpivots);
	}
	auto is_pivot = result->unpivot_names.empty();
	result->pivots = TransformPivotList(*root.pivots, is_pivot);
	if (!is_pivot && result->pivots.size() > 1) {
		throw ParserException("UNPIVOT requires a single pivot element");
	}
	if (root.groups) {
		result->groups = TransformStringList(root.groups);
	}
	for (auto &pivot : result->pivots) {
		if (!is_pivot) {
			// unpivot
			if (pivot.unpivot_names.size() != 1) {
				throw ParserException("UNPIVOT requires a single column name for the PIVOT IN clause");
			}
			D_ASSERT(pivot.pivot_expressions.empty());
		}
	}
	result->include_nulls = root.include_nulls;
	result->alias = TransformAlias(root.alias, result->column_name_alias);
	SetQueryLocation(*result, root.location);
	return std::move(result);
}

} // namespace duckdb
