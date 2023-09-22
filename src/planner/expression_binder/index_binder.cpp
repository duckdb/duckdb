#include "duckdb/planner/expression_binder/index_binder.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/core_functions/scalar/union_functions.hpp"

namespace duckdb {

IndexBinder::IndexBinder(Binder &binder, ClientContext &context, optional_ptr<TableCatalogEntry> table,
                         optional_ptr<CreateIndexInfo> info)
    : ExpressionBinder(binder, context), table(table), info(info) {
}

BindResult IndexBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in index expressions");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in index expressions");
	case ExpressionClass::COLUMN_REF: {

		if (!table) {
			return ExpressionBinder::BindExpression(expr_ptr, depth);
		}

		// WAL replay
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		auto field_name = col_ref.GetColumnName();
		const auto &columns = table->GetColumns();

		// we try to find the column name in the table
		auto column_in_table = false;
		const auto &column_names = columns.GetColumnNames();
		for (const auto &column_name : column_names) {
			if (column_name == field_name) {
				column_in_table = true;
			}
		}

		// we try to find the field name in a UNION
		auto is_union = false;
		if (!column_in_table) {

			// try to find a matching column (column name is second-last entry)
			D_ASSERT(col_ref.column_names.size() >= 2);
			auto column_name = col_ref.column_names[col_ref.column_names.size() - 2];
			const auto &column_types = columns.GetColumnTypes();

			for (idx_t i = 0; i < column_types.size(); i++) {
				if (column_names[i] == column_name && column_types[i].id() == LogicalTypeId::UNION) {

					// found a matching UNION column! Now check if it contains our tag
					auto &child_types = StructType::GetChildTypes(column_types[i]);
					for (const auto &child_type : child_types) {
						if (child_type.first == field_name) {
							is_union = true;
							break;
						}
					}
				}
			}
		}

		// get the logical index of the column in the table
		auto col_idx = is_union ? table->GetColumnIndex(col_ref.column_names[col_ref.column_names.size() - 2])
		                        : table->GetColumnIndex(col_ref.column_names.back());
		LogicalType col_type = table->GetColumn(col_idx).GetType();

		if (!col_idx.IsValid()) {
			throw InternalException("failed to replay CREATE INDEX statement - column id not found");
		}

		// find the col_idx in the index.column_ids
		auto col_id_idx = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < info->column_ids.size(); i++) {
			if (col_idx.index == info->column_ids[i]) {
				col_id_idx = i;
			}
		}
		D_ASSERT(col_id_idx != DConstants::INVALID_INDEX);

		// now create the bound column reference
		auto bound_col_ref_expr =
		    make_uniq<BoundColumnRefExpression>(field_name, col_type, ColumnBinding(0, col_id_idx));

		// we can just return that bound reference, if this is not a UNION
		if (!is_union) {
			return BindResult(std::move(bound_col_ref_expr));
		}

		// we need to create a UNION_EXTRACT on the UNION and return it
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(std::move(bound_col_ref_expr));
		arguments.push_back(make_uniq<BoundConstantExpression>(Value(field_name)));
		auto extract_function = UnionExtractFun::GetFunction();
		auto bind_info = extract_function.bind(context, extract_function, arguments);
		auto return_type = extract_function.return_type;
		auto bound_struct_extract = make_uniq<BoundFunctionExpression>(return_type, std::move(extract_function),
		                                                               std::move(arguments), std::move(bind_info));
		bound_struct_extract->alias = field_name;
		return BindResult(std::move(bound_struct_extract));
	}
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string IndexBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in index expressions";
}

} // namespace duckdb
