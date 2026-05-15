#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_qualifier.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/column_qualifier.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> ExpressionBinder::GetSQLValueFunction(const string &column_name) {
	return ColumnQualifier::GetSQLValueFunction(column_name);
}

unique_ptr<ParsedExpression> ExpressionBinder::CreateStructExtract(unique_ptr<ParsedExpression> base,
                                                                   const string &field_name) {
	ColumnQualifier qualifier(binder);
	return qualifier.CreateStructExtract(std::move(base), field_name);
}

unique_ptr<ParsedExpression> ExpressionBinder::CreateStructPack(ColumnRefExpression &col_ref) {
	ColumnQualifier qualifier(binder);
	return qualifier.CreateStructPack(col_ref);
}

void ExpressionBinder::QualifyColumnNames(Binder &binder, unique_ptr<ParsedExpression> &expr,
                                          optional_ptr<ColumnAliasBinder> alias_binder) {
	ColumnQualifier qualifier(binder, nullptr, alias_binder);
	vector<unordered_set<string>> lambda_params;
	qualifier.QualifyColumnNames(expr, lambda_params);
}

void ExpressionBinder::QualifyColumnNames(ExpressionBinder &expression_binder, unique_ptr<ParsedExpression> &expr) {
	ColumnQualifier qualifier(expression_binder.binder, expression_binder.lambda_bindings);
	vector<unordered_set<string>> lambda_params;
	qualifier.QualifyColumnNames(expr, lambda_params);
}

BindResult ExpressionBinder::BindExpression(LambdaRefExpression &lambda_ref, idx_t depth) {
	D_ASSERT(lambda_bindings && lambda_ref.lambda_idx < lambda_bindings->size());
	return (*lambda_bindings)[lambda_ref.lambda_idx].Bind(lambda_ref, depth);
}

unique_ptr<ParsedExpression> ExpressionBinder::QualifyColumnName(ColumnRefExpression &col_ref, ErrorData &error) {
	ColumnQualifier qualifier(binder, lambda_bindings);
	return qualifier.QualifyColumnName(col_ref, error);
}

BindResult ExpressionBinder::BindExpression(ColumnRefExpression &col_ref_p, idx_t depth, bool root_expression,
                                            unique_ptr<ParsedExpression> &expr_ptr) {
	if (binder.GetBindingMode() == BindingMode::EXTRACT_NAMES ||
	    binder.GetBindingMode() == BindingMode::EXTRACT_QUALIFIED_NAMES) {
		return BindResult(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}

	ErrorData error;
	auto expr = QualifyColumnName(col_ref_p, error);
	if (!expr) {
		// column wasn't found
		if (ExpressionBinder::IsPotentialAlias(col_ref_p)) {
			BindResult alias_result;
			auto found_alias = TryResolveAliasReference(col_ref_p, depth, root_expression, alias_result, expr_ptr);
			if (found_alias) {
				return alias_result;
			}

			auto value_function = GetSQLValueFunction(col_ref_p.GetColumnName());
			if (value_function) {
				return BindExpression(value_function, depth);
			}
		}
		error.AddQueryLocation(col_ref_p);
		return BindResult(std::move(error));
	}

	expr->SetQueryLocation(col_ref_p.GetQueryLocation());

	// the above QualifyColumnName returns a generated expression for a generated
	// column, and struct_extract for a struct, or a lambda reference expression,
	// all of them are not column reference expressions, so we return here
	if (expr->GetExpressionType() != ExpressionType::COLUMN_REF) {
		auto alias = expr->GetAlias();
		auto result = BindExpression(expr, depth);
		if (result.expression) {
			result.expression->SetAlias(std::move(alias));
		}
		return result;
	}

	// the above QualifyColumnName returned an individual column reference
	// expression, which we resolve to either a base table or a subquery expression,
	// and if it was a macro parameter, then we let macro_binding bind it to the argument
	BindResult result;
	auto &col_ref = expr->Cast<ColumnRefExpression>();
	D_ASSERT(col_ref.IsQualified());
	auto &table_name = col_ref.GetTableName();

	if (binder.macro_binding && table_name == binder.macro_binding->GetAlias()) {
		result = binder.macro_binding->Bind(col_ref, depth);
	} else {
		result = binder.bind_context.BindColumn(col_ref, depth);
	}

	if (result.HasError()) {
		result.error.AddQueryLocation(col_ref_p);
		return result;
	}

	// we bound the column reference
	BoundColumnReferenceInfo ref;
	ref.name = col_ref.column_names.back();
	ref.query_location = col_ref.GetQueryLocation();
	bound_columns.push_back(std::move(ref));
	return result;
}

} // namespace duckdb
