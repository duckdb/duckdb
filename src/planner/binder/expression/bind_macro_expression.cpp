#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"

namespace duckdb {

void ExpressionBinder::VisitMacroParametersInLambda(FunctionExpression &function,
                                                    vector<identifier_set_t> &lambda_params,
                                                    const macro_parameter_callback_t &callback) {
	for (auto &child : function.GetArgumentsMutable()) {
		if (child.GetExpression().GetExpressionClass() != ExpressionClass::LAMBDA) {
			VisitMacroParameters(child.GetExpressionMutable(), lambda_params, callback);
			continue;
		}

		// Special-handling for LHS lambda parameters.
		// We do not replace them, and we add them to the lambda_params vector.
		auto &lambda_expr = child.GetExpressionMutable()->Cast<LambdaExpression>();
		string error_message;
		auto column_ref_expressions = lambda_expr.ExtractColumnRefExpressions(error_message);

		if (!error_message.empty()) {
			// Possibly a JSON function, replace both LHS and RHS.
			VisitMacroParameters(lambda_expr.LeftMutable(), lambda_params, callback);
			VisitMacroParameters(lambda_expr.RightMutable(), lambda_params, callback);
			continue;
		}

		// Push the lambda parameter names of this level.
		lambda_params.emplace_back();
		for (const auto &column_ref_expr : column_ref_expressions) {
			const auto &column_ref = column_ref_expr.get().Cast<ColumnRefExpression>();
			lambda_params.back().emplace(column_ref.GetName());
		}

		// Only replace in the RHS of the expression.
		VisitMacroParameters(lambda_expr.RightMutable(), lambda_params, callback);

		lambda_params.pop_back();
	}
}

void ExpressionBinder::ReplaceMacroParameters(unique_ptr<ParsedExpression> &expr,
                                              vector<identifier_set_t> &lambda_params) {
	VisitMacroParameters(expr, lambda_params, [&](unique_ptr<ParsedExpression> &param, ColumnRefExpression &col_ref) {
		param = macro_binding->ParamToArg(col_ref);
	});
}

void ExpressionBinder::VisitMacroParameters(unique_ptr<ParsedExpression> &expr, vector<identifier_set_t> &lambda_params,
                                            const macro_parameter_callback_t &callback) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		// If the expression is a column reference, we replace it with its argument.
		auto &col_ref = expr->Cast<ColumnRefExpression>();
		if (LambdaExpression::IsLambdaParameter(lambda_params, col_ref.GetName())) {
			return;
		}

		bool bind_macro_parameter = false;
		if (col_ref.IsQualified()) {
			// the table qualifier is the component directly before the column name
			auto &names = col_ref.ColumnNames();
			if (names[names.size() - 2].StartsWith(DummyBinding::DUMMY_NAME)) {
				bind_macro_parameter = true;
			}
		} else {
			bind_macro_parameter = macro_binding->HasMatchingBinding(col_ref.GetColumnName());
		}

		if (bind_macro_parameter) {
			D_ASSERT(macro_binding->HasMatchingBinding(col_ref.GetColumnName()));
			callback(expr, col_ref);
		}
		return;
	}
	case ExpressionClass::FUNCTION: {
		// Special-handling for lambdas, which are inside function expressions.
		auto &function = expr->Cast<FunctionExpression>();
		if (function.IsLambdaFunction()) {
			return VisitMacroParametersInLambda(function, lambda_params, callback);
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		auto &sq = (expr->Cast<SubqueryExpression>()).Subquery();
		ParsedExpressionIterator::EnumerateQueryNodeChildren(*sq->node, [&](unique_ptr<ParsedExpression> &child) {
			VisitMacroParameters(child, lambda_params, callback);
		});
		break;
	}
	default:
		break;
	}

	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { VisitMacroParameters(child, lambda_params, callback); });
}

// Find aggregate expression children
void ExpressionBinder::FindAggregateExprs(unique_ptr<ParsedExpression> &expr,
                                          vector<reference<unique_ptr<ParsedExpression>>> &exprs) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &fn_expr = expr->Cast<FunctionExpression>();

		// Look up the function in the catalog, check to see if it is actually an aggregate function
		EntryLookupInfo fn_entry(CatalogType::AGGREGATE_FUNCTION_ENTRY, fn_expr.GetQualifiedName());
		auto entry = GetCatalogEntry(fn_entry, OnEntryNotFound::RETURN_NULL);

		if (entry && entry->type == CatalogType::AGGREGATE_FUNCTION_ENTRY) {
			exprs.push_back(expr);
			return;
		}
	}

	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child_expr) { FindAggregateExprs(child_expr, exprs); });
}

void ExpressionBinder::UnfoldWindowMacroExpression(unique_ptr<ParsedExpression> &expr, ScalarMacroFunction &macro_def) {
	auto macro_copy = macro_def.expression->Copy();
	vector<reference<unique_ptr<ParsedExpression>>> aggregate_exprs;
	FindAggregateExprs(macro_copy, aggregate_exprs);

	// Only allowed if the macro body has a single aggregate expression
	if (aggregate_exprs.size() != 1) {
		throw BinderException("Window function macro bodies must contain exactly one aggregate function");
	}

	// The window spec is pushed down to the aggregate function target within the macro body
	unique_ptr<ParsedExpression> &agg_expr_ref = aggregate_exprs[0];
	auto &agg_fn_expr = agg_expr_ref->Cast<FunctionExpression>();

	// Transfer the macro function attributes
	auto &window_expr = expr->Cast<WindowExpression>();
	window_expr.SetQualifiedName(agg_fn_expr.GetQualifiedName());
	window_expr.GetArgumentsMutable().clear();
	for (auto &arg : agg_fn_expr.GetArgumentsMutable()) {
		window_expr.GetArgumentsMutable().push_back(std::move(arg));
	}
	if (!window_expr.Distinct()) {
		window_expr.DistinctMutable() = agg_fn_expr.Distinct();
	}
	if (window_expr.Filter() && agg_fn_expr.Filter()) {
		// Two FILTER clauses: combine
		window_expr.FilterMutable() =
		    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(window_expr.FilterMutable()),
		                                     std::move(agg_fn_expr.FilterMutable()));
	} else if (agg_fn_expr.Filter()) {
		//	One FILTER from the MACRO
		window_expr.FilterMutable() = std::move(agg_fn_expr.FilterMutable());
	}
	// Transfer argument ORDER BYs
	if (agg_fn_expr.OrderBy()) {
		auto clone = agg_fn_expr.OrderBy()->Copy();
		window_expr.ArgOrdersMutable() = std::move(clone->Cast<OrderModifier>().orders);
	}

	// Replace the aggregate expression with the new window expression
	agg_expr_ref = std::move(expr);
	expr = std::move(macro_copy);
}

static bool IsTrivialMacroArgument(const ParsedExpression &arg) {
	switch (arg.GetExpressionClass()) {
	case ExpressionClass::CONSTANT:
	case ExpressionClass::COLUMN_REF:
	case ExpressionClass::PARAMETER:
		return true;
	default:
		return false;
	}
}

//! Whether the macro body can be evaluated inside a lambda, i.e. contains no aggregates, windows, subqueries, etc.
bool ExpressionBinder::CanEvaluateMacroInLambda(ParsedExpression &expr,
                                                reference_set_t<const ScalarMacroCatalogEntry> &visited_macros) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::SUBQUERY:
	case ExpressionClass::WINDOW:
	case ExpressionClass::STAR:
	case ExpressionClass::DEFAULT:
		return false;
	case ExpressionClass::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		if (function.IsLambdaFunction()) {
			break;
		}
		EntryLookupInfo function_lookup(CatalogType::SCALAR_FUNCTION_ENTRY, function.GetQualifiedName());
		auto entry = GetCatalogEntry(function_lookup, OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			return false;
		}
		if (entry->type == CatalogType::MACRO_ENTRY) {
			auto &macro_entry = entry->Cast<ScalarMacroCatalogEntry>();
			if (visited_macros.find(macro_entry) == visited_macros.end()) {
				visited_macros.insert(macro_entry);
				for (auto &macro : macro_entry.macros) {
					if (!CanEvaluateMacroInLambda(*macro->Cast<ScalarMacroFunction>().expression, visited_macros)) {
						return false;
					}
				}
			}
		} else if (entry->type != CatalogType::SCALAR_FUNCTION_ENTRY) {
			return false;
		}
		// UNNEST and GROUPING are bound by the binder of the surrounding clause
		auto &name = function.GetQualifiedName().Name();
		if (name == "unnest" || name == "unlist" || name == "grouping" || name == "grouping_id") {
			return false;
		}
		break;
	}
	default:
		break;
	}
	bool result = true;
	ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) {
		if (result && !CanEvaluateMacroInLambda(child, visited_macros)) {
			result = false;
		}
	});
	return result;
}

void ExpressionBinder::UnfoldMacroExpression(FunctionExpression &function, ScalarMacroCatalogEntry &macro_func,
                                             unique_ptr<ParsedExpression> &expr, idx_t depth) {
	// validate the arguments and separate positional and default arguments
	vector<unique_ptr<ParsedExpression>> positional_arguments;
	InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>> named_arguments;
	binder.lambda_bindings = lambda_bindings;
	auto bind_result = MacroFunction::BindMacroFunction(binder, macro_func.macros, macro_func.name, function,
	                                                    positional_arguments, named_arguments, depth);
	if (!bind_result.error.empty()) {
		throw BinderException(*expr, bind_result.error);
	}
	auto &macro_def = macro_func.macros[bind_result.function_idx.GetIndex()]->Cast<ScalarMacroFunction>();

	auto new_macro_binding =
	    MacroFunction::CreateDummyBinding(macro_def, macro_func.name, positional_arguments, named_arguments);
	macro_binding = new_macro_binding.get();

	// replace current expression with stored macro expression
	// special case: If this is a window function, then we need to return a window expression
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		UnfoldWindowMacroExpression(expr, macro_def);
	} else {
		expr = macro_def.expression->Copy();
	}

	// qualify only the macro parameters with a new empty binder that only knows the macro binding
	auto dummy_binder = Binder::CreateBinder(context);
	dummy_binder->macro_binding = new_macro_binding.get();
	ExpressionBinder::QualifyColumnNames(*dummy_binder, expr);

	// a parameter that is used multiple times would copy its argument for every use, which grows exponentially for
	// nested macros - instead, evaluate such arguments once and pass them to the body as lambda parameters
	auto &arguments = *macro_binding->arguments;
	vector<idx_t> use_counts(arguments.size(), 0);
	vector<identifier_set_t> lambda_params;
	VisitMacroParameters(expr, lambda_params, [&](unique_ptr<ParsedExpression> &, ColumnRefExpression &col_ref) {
		column_t column_index;
		if (macro_binding->TryGetBindingIndex(col_ref.GetColumnName(), column_index)) {
			use_counts[column_index]++;
		}
	});
	vector<string> lambda_names(arguments.size());
	vector<unique_ptr<ParsedExpression>> invoke_arguments;
	for (idx_t i = 0; i < arguments.size(); i++) {
		if (use_counts[i] > 1 && !IsTrivialMacroArgument(*arguments[i])) {
			lambda_names[i] = "__macro_argument_" + to_string(binder.global_binder_state->macro_argument_count++);
			invoke_arguments.push_back(arguments[i]->Copy());
		}
	}
	if (!invoke_arguments.empty() && expr->GetExpressionClass() != ExpressionClass::WINDOW) {
		reference_set_t<const ScalarMacroCatalogEntry> visited_macros;
		visited_macros.insert(macro_func);
		if (!CanEvaluateMacroInLambda(*expr, visited_macros)) {
			invoke_arguments.clear();
		}
	} else {
		invoke_arguments.clear();
	}

	// now replace the parameters
	VisitMacroParameters(expr, lambda_params, [&](unique_ptr<ParsedExpression> &param, ColumnRefExpression &col_ref) {
		column_t column_index;
		if (!invoke_arguments.empty() && macro_binding->TryGetBindingIndex(col_ref.GetColumnName(), column_index) &&
		    !lambda_names[column_index].empty()) {
			auto lambda_param = make_uniq<ColumnRefExpression>(Identifier(lambda_names[column_index]));
			lambda_param->SetAlias(col_ref.GetAlias());
			param = std::move(lambda_param);
			return;
		}
		param = macro_binding->ParamToArg(col_ref);
	});
	if (invoke_arguments.empty()) {
		return;
	}
	vector<string> parameter_names;
	for (auto &name : lambda_names) {
		if (!name.empty()) {
			parameter_names.push_back(name);
		}
	}
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<LambdaExpression>(std::move(parameter_names), std::move(expr)));
	for (auto &argument : invoke_arguments) {
		children.push_back(std::move(argument));
	}
	expr = make_uniq<FunctionExpression>(QualifiedName("system", "main", "invoke"), std::move(children));
}

BindResult ExpressionBinder::BindMacro(FunctionExpression &function, ScalarMacroCatalogEntry &macro_func, idx_t depth,
                                       unique_ptr<ParsedExpression> &expr) {
	auto stack_checker = StackCheck(*expr, 3);

	// unfold the macro expression
	UnfoldMacroExpression(function, macro_func, expr, depth);

	// bind the unfolded macro
	return BindExpression(expr, depth);
}

} // namespace duckdb
