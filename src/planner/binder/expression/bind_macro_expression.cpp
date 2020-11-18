#include "duckdb/planner/expression_binder.hpp"

#include "duckdb/catalog/catalog_entry/macro_function_catalog_entry.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/list.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> ExpressionBinder::UnfoldMacroRecursive(unique_ptr<ParsedExpression> expr) {
	auto macro_binding = make_unique<MacroBinding>(vector<LogicalType>(), vector<string>(), string());
	return UnfoldMacroRecursive(move(expr), *macro_binding);
}

unique_ptr<ParsedExpression> ExpressionBinder::UnfoldMacroRecursive(unique_ptr<ParsedExpression> expr,
                                                                    MacroBinding &macro_binding) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		// if expr is a parameter, replace it with its argument
		auto &colref = (ColumnRefExpression &)*expr;
		if (colref.table_name.empty() && macro_binding.HasMatchingBinding(colref.column_name)) {
			expr = macro_binding.ParamToArg(colref);
		}
		if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			return expr;
		} else {
			return UnfoldMacroRecursive(move(expr), macro_binding);
		}
	}
	case ExpressionClass::FUNCTION: {
		auto &function_expr = (FunctionExpression &)*expr;
		if (function_expr.is_operator)
			break;

		// if expr is a macro function, unfold it
		QueryErrorContext error_context(binder.root_statement, function_expr.query_location);
		auto &catalog = Catalog::GetCatalog(context);
		auto func = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, function_expr.schema,
		                             function_expr.function_name, true, error_context);
		if (func != nullptr && func->type == CatalogType::MACRO_ENTRY) {
			auto &macro_func = (MacroFunctionCatalogEntry &)*func;
			string error = MacroFunction::ValidateArguments(context, error_context, macro_func, function_expr);
			if (!error.empty())
				throw BinderException(binder.FormatError(*expr, error));

			// create macro_binding to bind this macro's parameters to its arguments
			vector<LogicalType> types;
			vector<string> names;
			for (idx_t i = 0; i < macro_func.function->parameters.size(); i++) {
				types.push_back(LogicalType::SQLNULL);
				auto &param = (ColumnRefExpression &)*macro_func.function->parameters[i];
				names.push_back(param.column_name);
			}
			auto new_macro_binding = make_unique<MacroBinding>(types, names, func->name);
			new_macro_binding->arguments = move(function_expr.children);

			expr = macro_func.function->expression->Copy();
			return UnfoldMacroRecursive(move(expr), *new_macro_binding);
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		// replacing parameters within a subquery is slightly different
		auto &sq = ((SubqueryExpression &)*expr).subquery;
		UnfoldQueryNode(*expr, *sq->node.get(), macro_binding);

		for (auto &kv : sq->cte_map) {
			UnfoldQueryNode(*expr, *kv.second->query->node.get(), macro_binding);
		}
		break;
	}
	default: // fall through
		break;
	}
	// unfold child expressions
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> child) -> unique_ptr<ParsedExpression> {
		    return UnfoldMacroRecursive(move(child), macro_binding);
	    });
	return expr;
}

void ExpressionBinder::UnfoldTableRef(ParsedExpression &expr, TableRef &ref, MacroBinding &macro_binding) {
	switch (ref.type) {
	case TableReferenceType::CROSS_PRODUCT: {
		auto &cp_ref = (CrossProductRef &)ref;
		UnfoldTableRef(expr, *cp_ref.left, macro_binding);
		UnfoldTableRef(expr, *cp_ref.right, macro_binding);
		break;
	}
	case TableReferenceType::EXPRESSION_LIST: {
		auto &el_ref = (ExpressionListRef &)ref;
		for (idx_t i = 0; i < el_ref.values.size(); i++) {
			for (idx_t j = 0; j < el_ref.values[i].size(); j++) {
				el_ref.values[i][j] = UnfoldMacroRecursive(move(el_ref.values[i][j]), macro_binding);
			}
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &j_ref = (JoinRef &)ref;
		UnfoldTableRef(expr, *j_ref.left, macro_binding);
		UnfoldTableRef(expr, *j_ref.right, macro_binding);
		j_ref.condition = UnfoldMacroRecursive(move(j_ref.condition), macro_binding);
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &sq_ref = (SubqueryRef &)ref;
		UnfoldQueryNode(expr, *sq_ref.subquery->node, macro_binding);
		for (auto &kv : sq_ref.subquery->cte_map) {
			UnfoldQueryNode(expr, *kv.second->query->node.get(), macro_binding);
		}
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf_ref = (TableFunctionRef &)ref;
		tf_ref.function = UnfoldMacroRecursive(move(tf_ref.function), macro_binding);
		break;
	}
	default:
		break;
	}
}

void ExpressionBinder::UnfoldQueryNode(ParsedExpression &expr, QueryNode &node, MacroBinding &macro_binding) {
	if (node.type != QueryNodeType::SELECT_NODE) {
		throw BinderException(binder.FormatError(expr, "Macro's with non-SELECT sub-queries are not supported."));
	}
	switch (node.type) {
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &rcte_node = (RecursiveCTENode &)node;
		UnfoldQueryNode(expr, *rcte_node.left, macro_binding);
		UnfoldQueryNode(expr, *rcte_node.right, macro_binding);
		break;
	}
	case QueryNodeType::SELECT_NODE: {
		auto &sel_node = (SelectNode &)node;
		for (idx_t i = 0; i < sel_node.select_list.size(); i++) {
			sel_node.select_list[i] = UnfoldMacroRecursive(move(sel_node.select_list[i]), macro_binding);
		}
		for (idx_t i = 0; i < sel_node.groups.size(); i++) {
			sel_node.groups[i] = UnfoldMacroRecursive(move(sel_node.groups[i]), macro_binding);
		}
		if (sel_node.where_clause != nullptr)
			sel_node.where_clause = UnfoldMacroRecursive(move(sel_node.where_clause), macro_binding);
		if (sel_node.having != nullptr)
			sel_node.having = UnfoldMacroRecursive(move(sel_node.having), macro_binding);

		if (sel_node.from_table->type == TableReferenceType::SUBQUERY) {
			auto &sq_ref = (SubqueryRef &)*sel_node.from_table;
			UnfoldQueryNode(expr, *sq_ref.subquery->node, macro_binding);
			for (auto &kv : sq_ref.subquery->cte_map) {
				UnfoldQueryNode(expr, *kv.second->query->node.get(), macro_binding);
			}
		} else if (sel_node.from_table->type == TableReferenceType::TABLE_FUNCTION) {
			auto &tf_ref = (TableFunctionRef &)*sel_node.from_table;
			tf_ref.function = UnfoldMacroRecursive(move(tf_ref.function), macro_binding);
		}
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop_node = (SetOperationNode &)node;
		UnfoldQueryNode(expr, *setop_node.left, macro_binding);
		UnfoldQueryNode(expr, *setop_node.right, macro_binding);
		break;
	}
	default:
		throw BinderException(binder.FormatError(expr, "Unsupported subquery type found in macro."));
	}
}

BindResult ExpressionBinder::BindMacro(FunctionExpression &expr) {
	string error;
	auto unfolded_expr = UnfoldMacroRecursive(expr.Copy());
	return BindExpression(*unfolded_expr, 0, true);
}

} // namespace duckdb
