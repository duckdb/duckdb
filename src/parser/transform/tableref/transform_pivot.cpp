#include "duckdb/common/exception.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

static void TransformPivotInList(unique_ptr<ParsedExpression> &expr, PivotColumnEntry &entry, bool root_entry = true) {
	if (expr->type == ExpressionType::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		if (colref.IsQualified()) {
			throw ParserException("PIVOT IN list cannot contain qualified column references");
		}
		entry.values.emplace_back(colref.GetColumnName());
	} else if (expr->type == ExpressionType::VALUE_CONSTANT) {
		auto &constant_expr = expr->Cast<ConstantExpression>();
		entry.values.push_back(std::move(constant_expr.value));
	} else if (root_entry && expr->type == ExpressionType::FUNCTION) {
		auto &function = expr->Cast<FunctionExpression>();
		if (function.function_name != "row") {
			throw ParserException("PIVOT IN list must contain columns or lists of columns");
		}
		for (auto &child : function.children) {
			TransformPivotInList(child, entry, false);
		}
	} else if (root_entry && expr->type == ExpressionType::STAR) {
		entry.star_expr = std::move(expr);
	} else {
		throw ParserException("PIVOT IN list must contain columns or lists of columns");
	}
}

PivotColumn Transformer::TransformPivotColumn(duckdb_libpgquery::PGPivot &pivot) {
	PivotColumn col;
	if (pivot.pivot_columns) {
		TransformExpressionList(*pivot.pivot_columns, col.pivot_expressions);
		for (auto &expr : col.pivot_expressions) {
			if (expr->IsScalar()) {
				throw ParserException("Cannot pivot on constant value \"%s\"", expr->ToString());
			}
			if (expr->HasSubquery()) {
				throw ParserException("Cannot pivot on subquery \"%s\"", expr->ToString());
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
			entry.alias = expr->alias;
			TransformPivotInList(expr, entry);
			col.entries.push_back(std::move(entry));
		}
	}
	if (pivot.subquery) {
		col.subquery = TransformSelectNode(*PGPointerCast<duckdb_libpgquery::PGSelectStmt>(pivot.subquery));
	}
	if (pivot.pivot_enum) {
		col.pivot_enum = pivot.pivot_enum;
	}
	return col;
}

vector<PivotColumn> Transformer::TransformPivotList(duckdb_libpgquery::PGList &list) {
	vector<PivotColumn> result;
	for (auto node = list.head; node != nullptr; node = node->next) {
		auto pivot = PGPointerCast<duckdb_libpgquery::PGPivot>(node->data.ptr_value);
		result.push_back(TransformPivotColumn(*pivot));
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
	result->pivots = TransformPivotList(*root.pivots);
	if (!result->unpivot_names.empty() && result->pivots.size() > 1) {
		throw ParserException("UNPIVOT requires a single pivot element");
	}
	if (root.groups) {
		result->groups = TransformStringList(root.groups);
	}
	for (auto &pivot : result->pivots) {
		idx_t expected_size;
		bool is_pivot = result->unpivot_names.empty();
		if (!result->unpivot_names.empty()) {
			// unpivot
			if (pivot.unpivot_names.size() != 1) {
				throw ParserException("UNPIVOT requires a single column name for the PIVOT IN clause");
			}
			D_ASSERT(pivot.pivot_expressions.empty());
			expected_size = pivot.entries[0].values.size();
		} else {
			// pivot
			expected_size = pivot.pivot_expressions.size();
			D_ASSERT(pivot.unpivot_names.empty());
		}
		for (auto &entry : pivot.entries) {
			if (entry.star_expr && is_pivot) {
				throw ParserException("PIVOT IN list must contain columns or lists of columns - star expressions are "
				                      "only supported for UNPIVOT");
			}
			if (entry.values.size() != expected_size) {
				throw ParserException("PIVOT IN list - inconsistent amount of rows - expected %d but got %d",
				                      expected_size, entry.values.size());
			}
		}
	}
	result->include_nulls = root.include_nulls;
	result->alias = TransformAlias(root.alias, result->column_name_alias);
	return std::move(result);
}

} // namespace duckdb
