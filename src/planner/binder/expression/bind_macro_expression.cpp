#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

void ExpressionBinder::ReplaceMacroParametersRecursive(unique_ptr<ParsedExpression> &expr) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		// if expr is a parameter, replace it with its argument
		auto &colref = expr->Cast<ColumnRefExpression>();
		bool bind_macro_parameter = false;
		if (colref.IsQualified()) {
			bind_macro_parameter = false;
			if (colref.GetTableName().find(DummyBinding::DUMMY_NAME) != string::npos) {
				bind_macro_parameter = true;
			}
		} else {
			bind_macro_parameter = macro_binding->HasMatchingBinding(colref.GetColumnName());
		}
		if (bind_macro_parameter) {
			D_ASSERT(macro_binding->HasMatchingBinding(colref.GetColumnName()));
			expr = macro_binding->ParamToArg(colref);
		}
		return;
	}
	case ExpressionClass::SUBQUERY: {
		// replacing parameters within a subquery is slightly different
		auto &sq = (expr->Cast<SubqueryExpression>()).subquery;
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    *sq->node, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParametersRecursive(child); });
		break;
	}
	default: // fall through
		break;
	}
	// unfold child expressions
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParametersRecursive(child); });
}

static void DetectInfiniteMacroRecursion(ClientContext &context, unique_ptr<ParsedExpression> &expr,
                                         reference_set_t<CatalogEntry> &expanded_macros) {
	optional_ptr<ScalarMacroCatalogEntry> recursive_macro;
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::FUNCTION: {
		auto &func = expr->Cast<FunctionExpression>();
		auto function = Catalog::GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, func.catalog, func.schema,
		                                  func.function_name, OnEntryNotFound::RETURN_NULL);
		if (function && function->type == CatalogType::MACRO_ENTRY) {
			if (expanded_macros.find(*function) != expanded_macros.end()) {
				throw BinderException("Infinite recursion detected in macro \"%s\"", func.function_name);
			} else {
				recursive_macro = &function->Cast<ScalarMacroCatalogEntry>();
			}
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		// replacing parameters within a subquery is slightly different
		auto &sq = (expr->Cast<SubqueryExpression>()).subquery;
		ParsedExpressionIterator::EnumerateQueryNodeChildren(*sq->node, [&](unique_ptr<ParsedExpression> &child) {
			DetectInfiniteMacroRecursion(context, child, expanded_macros);
		});
		break;
	}
	default: // fall through
		break;
	}
	// unfold child expressions
	if (recursive_macro) {
		auto &macro_def = recursive_macro->function->Cast<ScalarMacroFunction>();
		auto rec_expr = macro_def.expression->Copy();
		expanded_macros.insert(*recursive_macro);
		ParsedExpressionIterator::EnumerateChildren(*rec_expr, [&](unique_ptr<ParsedExpression> &child) {
			DetectInfiniteMacroRecursion(context, child, expanded_macros);
		});
		expanded_macros.erase(*recursive_macro);
	} else {
		ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
			DetectInfiniteMacroRecursion(context, child, expanded_macros);
		});
	}
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

	// detect infinite recursion
	reference_set_t<CatalogEntry> expanded_macros;
	expanded_macros.insert(macro_func);
	DetectInfiniteMacroRecursion(context, expr, expanded_macros);

	// now replace the parameters
	ReplaceMacroParametersRecursive(expr);

	// bind the unfolded macro
	return BindExpression(expr, depth);
}

} // namespace duckdb
