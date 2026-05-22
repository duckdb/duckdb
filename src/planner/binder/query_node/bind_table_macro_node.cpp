#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/function/table_macro_function.hpp"
#include "duckdb/function/scalar_macro_function.hpp"

namespace duckdb {

unique_ptr<QueryNode> Binder::BindTableMacro(FunctionExpression &function, MacroCatalogEntry &macro_func, idx_t depth) {
	// validate the arguments and separate positional and default arguments
	vector<unique_ptr<ParsedExpression>> positional_arguments;
	InsertionOrderPreservingMap<unique_ptr<ParsedExpression>> named_arguments;

	auto bind_result = MacroFunction::BindMacroFunction(*this, macro_func.macros, macro_func.name, function,
	                                                    positional_arguments, named_arguments, depth);
	if (!bind_result.error.empty()) {
		throw BinderException(function, bind_result.error);
	}
	auto &macro_def = *macro_func.macros[bind_result.function_idx.GetIndex()];

	auto new_macro_binding =
	    MacroFunction::CreateDummyBinding(macro_def, macro_func.name, positional_arguments, named_arguments);
	new_macro_binding->arguments = &positional_arguments;

	// We need an ExpressionBinder so that we can call ExpressionBinder::ReplaceMacroParametersRecursive()
	auto eb = ExpressionBinder(*this, this->context);

	eb.macro_binding = new_macro_binding.get();
	vector<unordered_set<string>> lambda_params;

	unique_ptr<QueryNode> node;
	if (macro_def.type == MacroType::SCALAR_MACRO) {
		auto select_node = make_uniq<SelectNode>();
		auto expr = macro_def.Cast<ScalarMacroFunction>().expression->Copy();
		expr->SetAlias(macro_func.name);
		select_node->select_list.push_back(std::move(expr));
		select_node->from_table = make_uniq<EmptyTableRef>();
		node = std::move(select_node);
	} else {
		node = macro_def.Cast<TableMacroFunction>().query_node->Copy();
		// PG-compat: RETURNS TABLE(name type, ...) declares per-column names that
		// must be applied to the underlying SELECT's projection. The macro body
		// produces unnamed columns (e.g., SELECT NULL::TEXT, NULL::TEXT) and
		// callers reference them by the RETURNS TABLE names.
		if (!macro_def.return_names.empty() && node->type == QueryNodeType::SELECT_NODE) {
			auto &select_node = node->Cast<SelectNode>();
			if (select_node.select_list.size() == macro_def.return_names.size()) {
				for (idx_t i = 0; i < macro_def.return_names.size(); i++) {
					select_node.select_list[i]->SetAlias(macro_def.return_names[i]);
				}
			}
		} else if (macro_def.return_names.empty() && macro_def.return_types.size() == 1 &&
		           node->type == QueryNodeType::SELECT_NODE) {
			// PG-compat: scalar RETURNS <type> body re-parsed as SELECT. The
			// caller expects the column to be named after the function.
			auto &select_node = node->Cast<SelectNode>();
			if (select_node.select_list.size() == 1) {
				select_node.select_list[0]->SetAlias(macro_func.name);
			}
		}
	}
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    *node, [&](unique_ptr<ParsedExpression> &child) { eb.ReplaceMacroParameters(child, lambda_params); });

	return node;
}

} // namespace duckdb
