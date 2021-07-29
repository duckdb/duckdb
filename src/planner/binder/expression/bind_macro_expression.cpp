#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/list.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void ExpressionBinder::ReplaceMacroParametersRecursive(unique_ptr<ParsedExpression> &expr) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		// if expr is a parameter, replace it with its argument
		auto &colref = (ColumnRefExpression &)*expr;
		if (colref.table_name.empty() && macro_binding->HasMatchingBinding(colref.column_name)) {
			expr = macro_binding->ParamToArg(colref);
		}
		return;
	}
	case ExpressionClass::SUBQUERY: {
		// replacing parameters within a subquery is slightly different
		auto &sq = ((SubqueryExpression &)*expr).subquery;
		ReplaceMacroParametersRecursive(*expr, *sq->node);
		break;
	}
	default: // fall through
		break;
	}
	// unfold child expressions
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParametersRecursive(child); });
}

void ExpressionBinder::ReplaceMacroParametersRecursive(ParsedExpression &expr, TableRef &ref) {
	switch (ref.type) {
	case TableReferenceType::CROSS_PRODUCT: {
		auto &cp_ref = (CrossProductRef &)ref;
		ReplaceMacroParametersRecursive(expr, *cp_ref.left);
		ReplaceMacroParametersRecursive(expr, *cp_ref.right);
		break;
	}
	case TableReferenceType::EXPRESSION_LIST: {
		auto &el_ref = (ExpressionListRef &)ref;
		for (idx_t i = 0; i < el_ref.values.size(); i++) {
			for (idx_t j = 0; j < el_ref.values[i].size(); j++) {
				ReplaceMacroParametersRecursive(el_ref.values[i][j]);
			}
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &j_ref = (JoinRef &)ref;
		ReplaceMacroParametersRecursive(expr, *j_ref.left);
		ReplaceMacroParametersRecursive(expr, *j_ref.right);
		ReplaceMacroParametersRecursive(j_ref.condition);
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &sq_ref = (SubqueryRef &)ref;
		ReplaceMacroParametersRecursive(expr, *sq_ref.subquery->node);
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf_ref = (TableFunctionRef &)ref;
		ReplaceMacroParametersRecursive(tf_ref.function);
		break;
	}
	case TableReferenceType::BASE_TABLE:
	case TableReferenceType::EMPTY:
		// these TableRefs do not need to be unfolded
		break;
	default:
		throw NotImplementedException("TableRef type not implemented for macro's!");
	}
}

void ExpressionBinder::ReplaceMacroParametersRecursive(ParsedExpression &expr, QueryNode &node) {
	switch (node.type) {
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &rcte_node = (RecursiveCTENode &)node;
		ReplaceMacroParametersRecursive(expr, *rcte_node.left);
		ReplaceMacroParametersRecursive(expr, *rcte_node.right);
		break;
	}
	case QueryNodeType::SELECT_NODE: {
		auto &sel_node = (SelectNode &)node;
		for (idx_t i = 0; i < sel_node.select_list.size(); i++) {
			ReplaceMacroParametersRecursive(sel_node.select_list[i]);
		}
		for (idx_t i = 0; i < sel_node.groups.size(); i++) {
			ReplaceMacroParametersRecursive(sel_node.groups[i]);
		}
		if (sel_node.where_clause != nullptr) {
			ReplaceMacroParametersRecursive(sel_node.where_clause);
		}
		if (sel_node.having != nullptr) {
			ReplaceMacroParametersRecursive(sel_node.having);
		}

		ReplaceMacroParametersRecursive(expr, *sel_node.from_table.get());
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop_node = (SetOperationNode &)node;
		ReplaceMacroParametersRecursive(expr, *setop_node.left);
		ReplaceMacroParametersRecursive(expr, *setop_node.right);
		break;
	}
	default:
		throw NotImplementedException("QueryNode type not implemented for macro's!");
	}
	for (auto &kv : node.cte_map) {
		ReplaceMacroParametersRecursive(expr, *kv.second->query->node);
	}
}

BindResult ExpressionBinder::BindMacro(FunctionExpression &function, MacroCatalogEntry *macro_func, idx_t depth,
                                       unique_ptr<ParsedExpression> *expr) {
	auto &macro_def = *macro_func->function;
	// validate the arguments and separate positional and default arguments
	vector<unique_ptr<ParsedExpression>> positionals;
	unordered_map<string, unique_ptr<ParsedExpression>> defaults;
	string error = MacroFunction::ValidateArguments(*macro_func, function, positionals, defaults);
	if (!error.empty()) {
		return BindResult(binder.FormatError(*expr->get(), error));
	}

	// create a MacroBinding to bind this macro's parameters to its arguments
	vector<LogicalType> types;
	vector<string> names;
	// positional parameters
	for (idx_t i = 0; i < macro_def.parameters.size(); i++) {
		types.push_back(LogicalType::SQLNULL);
		auto &param = (ColumnRefExpression &)*macro_def.parameters[i];
		names.push_back(param.column_name);
	}
	// default parameters
	for (auto it = macro_def.default_parameters.begin(); it != macro_def.default_parameters.end(); it++) {
		types.push_back(LogicalType::SQLNULL);
		names.push_back(it->first);
		// now push the defaults into the positionals
		positionals.push_back(move(defaults[it->first]));
	}
	auto new_macro_binding = make_unique<MacroBinding>(types, names, macro_func->name);
	new_macro_binding->arguments = move(positionals);
	macro_binding = new_macro_binding.get();

	// replace current expression with stored macro expression, and replace params
	*expr = macro_func->function->expression->Copy();
	ReplaceMacroParametersRecursive(*expr);

	// bind the unfolded macro
	return BindExpression(expr, depth);
}

} // namespace duckdb
