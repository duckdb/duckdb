#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

bool IsLambdaParameter(vector<unordered_set<string>> &lambda_params, ColumnRefExpression &col_ref) {
	for (const auto &level : lambda_params) {
		if (level.find(col_ref.GetColumnName()) != level.end()) {
			return true;
		}
	}
	return false;
}

void ExpressionBinder::ReplaceMacroParamInLambda(unique_ptr<ParsedExpression> &expr,
                                                 vector<unordered_set<string>> &lambda_params) {

	// special-handling for LHS lambda parameters
	// we do not replace them, and we add them to the lambda_params vector
	auto &lambda_expr = expr->Cast<LambdaExpression>();
	lambda_params.emplace_back();

	// single parameter, add to lambda_params
	if (lambda_expr.lhs->expression_class == ExpressionClass::COLUMN_REF) {
		auto column_ref = lambda_expr.lhs->Cast<ColumnRefExpression>();
		lambda_params.back().emplace(column_ref.GetColumnName());

	} else {
		// multiple lambda parameters
		auto &func_expr = lambda_expr.lhs->Cast<FunctionExpression>();
		for (const auto &column_ref_expr : func_expr.children) {
			auto column_ref = column_ref_expr->Cast<ColumnRefExpression>();
			lambda_params.back().emplace(column_ref.GetColumnName());
		}
	}

	// only replace in RHS
	ParsedExpressionIterator::EnumerateChildren(
	    *lambda_expr.expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParameters(child, lambda_params); });

	// pop this level
	lambda_params.pop_back();
}

bool ExpressionBinder::MacroLambdaParamReplacer(unique_ptr<ParsedExpression> &expr,
                                                vector<unordered_set<string>> &lambda_params) {

	// early-out, if not a FUNCTION
	if (expr->GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}

	auto &function = expr->Cast<FunctionExpression>();

	// early-out for UNNEST
	if (function.function_name == UNNEST_FUNCTION_ALIAS || function.function_name == UNLIST_FUNCTION_ALIAS) {
		return false;
	}

	// early-out, if not in the catalog
	QueryErrorContext error_context(binder.root_statement, function.query_location);
	binder.BindSchemaOrCatalog(function.catalog, function.schema);
	auto func = Catalog::GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, function.catalog, function.schema,
	                              function.function_name, OnEntryNotFound::RETURN_NULL, error_context);
	if (!func) {
		return false;
	}

	// early-out, if not a SCALAR_FUNCTION_ENTRY
	if (func->type != CatalogType::SCALAR_FUNCTION_ENTRY) {
		return false;
	}

	// early-out, if JSON extension operator
	if (function.function_name == "->>") {
		return false;
	}

	// early-out, if not a lambda function
	bool has_lambda = false;
	for (auto &child : function.children) {
		if (child->expression_class == ExpressionClass::LAMBDA) {
			has_lambda = true;
			break;
		}
	}
	if (!has_lambda) {
		return false;
	}

	// early-out, if not an actual lambda function, i.e., a JSON function
	auto &lambda_function = func->Cast<ScalarFunctionCatalogEntry>();
	D_ASSERT(lambda_function.functions.functions.size() == 1);
	auto &scalar_function = lambda_function.functions.functions.front();
	auto &bind_lambda_function = scalar_function.bind_lambda;
	if (!bind_lambda_function) {
		return false;
	}

	// now replace the children
	for (idx_t i = 0; i < function.children.size(); i++) {
		if (function.children[i]->expression_class == ExpressionClass::LAMBDA) {
			ReplaceMacroParamInLambda(function.children[i], lambda_params);
		} else {
			ReplaceMacroParameters(function.children[i], lambda_params);
		}
	}

	return true;
}

void ExpressionBinder::ReplaceMacroParameters(unique_ptr<ParsedExpression> &expr,
                                              vector<unordered_set<string>> &lambda_params) {

	// special-handling for lambdas, which are inside function expressions,
	// we need to perform some checks here, to ensure that it is indeed a lambda function,
	// and not a JSON function
	if (MacroLambdaParamReplacer(expr, lambda_params)) {
		return;
	}

	switch (expr->GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		// if the expression is a parameter, replace it with its argument
		auto &col_ref = expr->Cast<ColumnRefExpression>();

		// don't replace lambda parameters
		if (IsLambdaParameter(lambda_params, col_ref)) {
			return;
		}

		bool bind_macro_parameter = false;
		if (col_ref.IsQualified()) {
			if (col_ref.GetTableName().find(DummyBinding::DUMMY_NAME) != string::npos) {
				bind_macro_parameter = true;
			}
		} else {
			bind_macro_parameter = macro_binding->HasMatchingBinding(col_ref.GetColumnName());
		}

		if (bind_macro_parameter) {
			D_ASSERT(macro_binding->HasMatchingBinding(col_ref.GetColumnName()));
			expr = macro_binding->ParamToArg(col_ref);
		}
		return;
	}
	case ExpressionClass::SUBQUERY: {
		auto &sq = (expr->Cast<SubqueryExpression>()).subquery;
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    *sq->node, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParameters(child, lambda_params); });
		break;
	}
	default: // fall through
		break;
	}

	// replace macro parameters in child expressions
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParameters(child, lambda_params); });
}

BindResult ExpressionBinder::BindMacro(FunctionExpression &function, ScalarMacroCatalogEntry &macro_func, idx_t depth,
                                       unique_ptr<ParsedExpression> &expr) {

	// recast function so we can access the scalar member function->expression
	auto &macro_def = macro_func.function->Cast<ScalarMacroFunction>();

	// validate the arguments and separate positional and default arguments
	vector<unique_ptr<ParsedExpression>> positionals;
	unordered_map<string, unique_ptr<ParsedExpression>> defaults;

	string error =
	    MacroFunction::ValidateArguments(*macro_func.function, macro_func.name, function, positionals, defaults);
	if (!error.empty()) {
		throw BinderException(binder.FormatError(*expr, error));
	}

	// create a MacroBinding to bind this macro's parameters to its arguments
	vector<LogicalType> types;
	vector<string> names;
	// positional parameters
	for (idx_t i = 0; i < macro_def.parameters.size(); i++) {
		types.emplace_back(LogicalType::SQLNULL);
		auto &param = macro_def.parameters[i]->Cast<ColumnRefExpression>();
		names.push_back(param.GetColumnName());
	}
	// default parameters
	for (auto it = macro_def.default_parameters.begin(); it != macro_def.default_parameters.end(); it++) {
		types.emplace_back(LogicalType::SQLNULL);
		names.push_back(it->first);
		// now push the defaults into the positionals
		positionals.push_back(std::move(defaults[it->first]));
	}
	auto new_macro_binding = make_uniq<DummyBinding>(types, names, macro_func.name);
	new_macro_binding->arguments = &positionals;
	macro_binding = new_macro_binding.get();

	// replace current expression with stored macro expression
	expr = macro_def.expression->Copy();

	// now replace the parameters
	vector<unordered_set<string>> lambda_params;
	ReplaceMacroParameters(expr, lambda_params);

	// bind the unfolded macro
	return BindExpression(expr, depth);
}

} // namespace duckdb
